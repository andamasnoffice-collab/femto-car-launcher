package io.github.seijikohara.femto.data.map

import android.content.ContentResolver
import android.database.sqlite.SQLiteDatabase
import android.net.Uri
import android.os.StatFs
import java.io.File
import java.io.FileOutputStream
import java.util.UUID
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/**
 * Owns user-imported MBTiles packages in private app storage.
 *
 * Import is transactional: the source is copied to a staging file, its SQLite
 * schema and tile payload are validated, and only then is it promoted to the
 * visible package filename. A failed or interrupted import never appears in
 * [listPackages].
 */
internal class MbTilesPackageStore(
    private val contentResolver: ContentResolver,
    filesDirectory: File,
) {
    private val packageDirectory = File(filesDirectory, PACKAGE_DIRECTORY)

    suspend fun importPackage(
        source: Uri,
        suggestedName: String,
    ): MbTilesPackage = withContext(Dispatchers.IO) {
        packageDirectory.mkdirs().also { created ->
            check(created || packageDirectory.isDirectory) { "Offline map storage could not be created." }
        }

        val id = UUID.randomUUID().toString()
        val stagingFile = File(packageDirectory, ".$id.importing")
        val packageFile = File(packageDirectory, "$id.mbtiles")
        try {
            contentResolver.openInputStream(source)?.use { input ->
                FileOutputStream(stagingFile).use { output ->
                    val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
                    var totalBytes = 0L
                    while (true) {
                        val count = input.read(buffer)
                        if (count < 0) break
                        totalBytes += count
                        require(totalBytes <= MAX_PACKAGE_BYTES) {
                            "The MBTiles package exceeds the 2 GB import limit."
                        }
                        require(availableBytes() >= count + MIN_FREE_SPACE_BYTES) {
                            "There is not enough free storage for this offline map."
                        }
                        output.write(buffer, 0, count)
                    }
                    output.fd.sync()
                }
            } ?: error("The selected file could not be opened.")
            val metadata = readAndValidate(stagingFile, suggestedName)
            check(stagingFile.renameTo(packageFile)) { "The offline map could not be saved." }
            metadata.copy(id = id, fileName = packageFile.name)
        } catch (error: Exception) {
            stagingFile.delete()
            packageFile.delete()
            throw error
        }
    }

    suspend fun listPackages(): List<MbTilesPackage> = withContext(Dispatchers.IO) {
        if (!packageDirectory.isDirectory) return@withContext emptyList()
        packageDirectory.listFiles()
            .orEmpty()
            .asSequence()
            .filter { it.isFile && it.extension == FILE_EXTENSION }
            .mapNotNull { file ->
                runCatching {
                    readAndValidate(file, file.nameWithoutExtension).copy(
                        id = file.nameWithoutExtension,
                        fileName = file.name,
                    )
                }.getOrNull()
            }
            .sortedBy { it.name.lowercase() }
            .toList()
    }

    suspend fun deletePackage(id: String): Boolean = withContext(Dispatchers.IO) {
        val safeId = runCatching { UUID.fromString(id).toString() }.getOrNull()
            ?: return@withContext false
        File(packageDirectory, "$safeId.$FILE_EXTENSION").delete()
    }

    /**
     * Reads one tile using MapLibre's XYZ coordinates. Standard MBTiles
     * packages store rows in TMS order, so the Y coordinate is flipped.
     * A database handle is scoped to one request: WebView tile requests may
     * run concurrently and no SQLite connection is shared across threads.
     */
    suspend fun readTile(
        id: String,
        zoom: Int,
        x: Int,
        y: Int,
    ): MbTilesTile? = withContext(Dispatchers.IO) {
        val safeId = runCatching { UUID.fromString(id).toString() }.getOrNull()
            ?: return@withContext null
        if (zoom !in 0..30) return@withContext null
        val tileCountAtZoom = 1L shl zoom
        if (x < 0 || x.toLong() >= tileCountAtZoom || y < 0 || y.toLong() >= tileCountAtZoom) {
            return@withContext null
        }
        val file = File(packageDirectory, "$safeId.$FILE_EXTENSION")
        if (!file.isFile) return@withContext null
        SQLiteDatabase.openDatabase(
            file.absolutePath,
            null,
            SQLiteDatabase.OPEN_READONLY or SQLiteDatabase.NO_LOCALIZED_COLLATORS,
        ).use { database ->
            val metadata = database.rawQuery("SELECT value FROM metadata WHERE name = 'scheme'", null).use { cursor ->
                if (cursor.moveToFirst()) cursor.getString(0)?.lowercase() else null
            }
            val row = if (metadata == "xyz") y else tileCountAtZoom.toInt() - 1 - y
            database.rawQuery(
                "SELECT tile_data FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ? LIMIT 1",
                arrayOf(zoom.toString(), x.toString(), row.toString()),
            ).use { cursor ->
                if (!cursor.moveToFirst() || cursor.isNull(0)) return@withContext null
                val bytes = cursor.getBlob(0)
                if (bytes.isEmpty()) return@withContext null
                val format = database.rawQuery("SELECT value FROM metadata WHERE name = 'format'", null).use { meta ->
                    if (meta.moveToFirst()) meta.getString(0)?.lowercase() else null
                }
                MbTilesTile(bytes, format?.let(::tileMimeType) ?: "application/octet-stream")
            }
        }
    }

    private fun tileMimeType(format: String): String = when (format) {
        "png" -> "image/png"
        "jpg", "jpeg" -> "image/jpeg"
        "webp" -> "image/webp"
        "pbf", "mvt" -> "application/x-protobuf"
        else -> "application/octet-stream"
    }

    private fun readAndValidate(
        file: File,
        fallbackName: String,
    ): MbTilesPackage = SQLiteDatabase.openDatabase(
        file.absolutePath,
        null,
        SQLiteDatabase.OPEN_READONLY,
    ).use { database ->
        val integrity = database.rawQuery("PRAGMA quick_check(1)", null).use { cursor ->
            cursor.moveToFirst() && cursor.getString(0) == "ok"
        }
        require(integrity) { "The MBTiles database is damaged and cannot be imported." }

        val tables = database.rawQuery(
            "SELECT name FROM sqlite_master WHERE type = 'table'",
            null,
        ).use { cursor ->
            buildSet {
                while (cursor.moveToNext()) add(cursor.getString(0))
            }
        }
        require("metadata" in tables && "tiles" in tables) {
            "This file is not a valid MBTiles database."
        }

        val metadataColumns = tableColumns(database, "metadata")
        val tileColumns = tableColumns(database, "tiles")
        require(setOf("name", "value").all(metadataColumns::contains)) {
            "The MBTiles metadata table is incomplete."
        }
        require(setOf("zoom_level", "tile_column", "tile_row", "tile_data").all(tileColumns::contains)) {
            "The MBTiles tile table is incomplete."
        }

        val values = database.rawQuery("SELECT name, value FROM metadata", null).use { cursor ->
            buildMap {
                while (cursor.moveToNext()) put(cursor.getString(0), cursor.getString(1))
            }
        }
        val format = values["format"]?.lowercase().orEmpty()
        require(format in SUPPORTED_FORMATS) {
            "Unsupported MBTiles format: ${format.ifBlank { "unknown" }}."
        }
        val tileCount = database.rawQuery("SELECT COUNT(*) FROM tiles", null).use { cursor ->
            require(cursor.moveToFirst())
            cursor.getLong(0)
        }
        require(tileCount > 0) { "The MBTiles file contains no map tiles." }
        database.rawQuery("SELECT tile_data FROM tiles LIMIT 1", null).use { cursor ->
            require(cursor.moveToFirst() && !cursor.isNull(0) && cursor.getBlob(0).isNotEmpty()) {
                "The MBTiles file contains an empty tile payload."
            }
        }

        MbTilesPackage(
            id = "",
            name = values["name"]?.takeIf(String::isNotBlank) ?: fallbackName
                .substringAfterLast('/')
                .removeSuffix(".$FILE_EXTENSION")
                .filterNot(Char::isISOControl)
                .trim()
                .take(MAX_PACKAGE_NAME_LENGTH)
                .ifBlank { "Offline map" },
            fileName = file.name,
            format = format,
            minZoom = values["minzoom"]?.toIntOrNull(),
            maxZoom = values["maxzoom"]?.toIntOrNull(),
            bounds = values["bounds"],
            attribution = values["attribution"],
            tileCount = tileCount,
            fileSizeBytes = file.length(),
            scheme = values["scheme"]?.lowercase()?.takeIf { it == "xyz" || it == "tms" } ?: "tms",
        )
    }

    private fun tableColumns(
        database: SQLiteDatabase,
        table: String,
    ): Set<String> = database.rawQuery("PRAGMA table_info($table)", null).use { cursor ->
        buildSet {
            while (cursor.moveToNext()) add(cursor.getString(cursor.getColumnIndexOrThrow("name")))
        }
    }

    private fun availableBytes(): Long = StatFs(packageDirectory.absolutePath).availableBytes

    private companion object {
        const val PACKAGE_DIRECTORY = "offline-maps"
        const val FILE_EXTENSION = "mbtiles"
        const val MAX_PACKAGE_BYTES = 2L * 1024 * 1024 * 1024
        const val MIN_FREE_SPACE_BYTES = 64L * 1024 * 1024
        const val MAX_PACKAGE_NAME_LENGTH = 80
        val SUPPORTED_FORMATS = setOf("png", "jpg", "jpeg", "webp", "pbf", "mvt")
    }
}

internal data class MbTilesPackage(
    val id: String,
    val name: String,
    val fileName: String,
    val format: String,
    val minZoom: Int?,
    val maxZoom: Int?,
    val bounds: String?,
    val attribution: String?,
    val tileCount: Long,
    val fileSizeBytes: Long,
    val scheme: String,
)

/** A single MBTiles tile payload and its declared media type. */
internal data class MbTilesTile(
    val bytes: ByteArray,
    val contentType: String,
)
