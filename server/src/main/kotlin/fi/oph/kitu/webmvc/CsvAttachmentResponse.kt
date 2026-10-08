package fi.oph.kitu.webmvc

import fi.oph.kitu.csvparsing.writeExcelCsvPrelude
import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.html.table.DisplayTableCsvRenderer
import fi.oph.kitu.html.table.RenderableDisplayTableEnum
import fi.oph.kitu.i18n.CurrentLanguage
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody

fun buildCsvFilename(
    prefix: String,
    piilotaHenkilotiedot: Boolean,
    vararg segments: String?,
): String =
    (listOf(prefix, if (piilotaHenkilotiedot) null else "henkilotiedot") + segments)
        .filterNotNull()
        .joinToString("_", postfix = ".csv")

inline fun <reified C : Enum<C>, T> csvAttachmentResponse(
    filename: String,
    data: Iterable<T>,
    excludeTags: Set<ColumnTag> = emptySet(),
    noinline includeColumn: (RenderableDisplayTableEnum<T>) -> Boolean = { true },
): ResponseEntity<StreamingResponseBody> {
    val language = CurrentLanguage.get()
    return ResponseEntity
        .ok()
        .contentType(MediaType.parseMediaType("text/csv"))
        .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=$filename")
        .body(
            StreamingResponseBody { output ->
                CurrentLanguage.withLanguage(language) {
                    output.writeExcelCsvPrelude()
                    DisplayTableCsvRenderer.renderCsv<C, _>(
                        output = output,
                        data = data,
                        excludeTags = excludeTags,
                        includeColumn = includeColumn,
                    )
                }
            },
        )
}
