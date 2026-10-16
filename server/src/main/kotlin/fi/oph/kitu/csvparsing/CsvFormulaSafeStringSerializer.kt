package fi.oph.kitu.csvparsing

import fi.oph.kitu.html.table.DisplayTableCsvRenderer
import tools.jackson.core.JsonGenerator
import tools.jackson.databind.SerializationContext
import tools.jackson.databind.ValueSerializer

/**
 * Prefixes a single tick (`'`) onto String values that begin with one of the
 * spreadsheet-formula trigger characters, so a malicious value persisted via
 * CSV import is not evaluated as a formula when a virkailija opens the export
 * in Excel/LibreOffice/Numbers. See OWASP "CSV injection".
 */
class CsvFormulaSafeStringSerializer : ValueSerializer<String>() {
    override fun serialize(
        value: String?,
        jsonGenerator: JsonGenerator?,
        serializationContext: SerializationContext,
    ) {
        jsonGenerator?.writeString(
            value?.let { sanitize(it) },
        )
    }

    companion object {
        fun sanitize(value: String): String = DisplayTableCsvRenderer.neutralizeFormula(value)
    }
}
