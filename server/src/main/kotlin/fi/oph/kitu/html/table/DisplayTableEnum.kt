package fi.oph.kitu.html.table

import fi.oph.kitu.i18n.CurrentLanguage
import fi.oph.kitu.i18n.LocalizedString
import kotlinx.html.FlowContent
import java.io.OutputStream
import kotlin.collections.filterIsInstance

interface DisplayTableEnum {
    val name: String
    val entityName: String?
    val uiHeaderValue: LocalizedString
    val urlParam: String

    fun <T> withValue(
        getValue: (T) -> String,
        renderHtml: (FlowContent.(T) -> Unit)? = null,
    ): DisplayTableColumn<T> =
        DisplayTableColumn(
            label = uiHeaderValue.toString(),
            sortKey = urlParam,
            getValue = getValue,
            renderHtml = renderHtml,
            testId = entityName,
        )

    fun <T> withHtml(renderHtml: (FlowContent.(T) -> Unit)): DisplayTableColumn<T> =
        DisplayTableColumn(
            label = uiHeaderValue.toString(),
            sortKey = urlParam,
            renderHtml = renderHtml,
            testId = entityName,
        )
}

/**
 * Edustaa taulukon sarakkeita, jotka voi renderöidä HTML- tai CSV-muotoon.
 * Tyyppi T on dataluokka, josta sarakkeiden datakenttien sisällöt luetaan.
 *
 * CSV-tulostukseen mukaan otettavat sarakkeet on annotoitava @ColumnTags(CSV_EXPORT)
 * HTML-tulostukseen mukaan otettavat sarakkeet on annotoitava @ColumnTags(LIST_VIEW)
 */
interface RenderableDisplayTableEnum<T> : DisplayTableEnum {
    /**
     * Palauttaa arvon merkkijonona, jota käytetään CSV-taulussa sekä HTML-muodossa, jos renderHtml on null.
     */
    val getValue: (value: T) -> String

    /**
     * Renderöi datan HTML-muodossa. Jos tämä on null, getValuen palauttama arvo tulostetaan.
     */
    val renderHtml: ((parent: FlowContent, value: T) -> Unit)?

    companion object {
        inline fun <reified C : Enum<C>, T> getByTags(
            require: Set<ColumnTag>,
            exclude: Set<ColumnTag> = emptySet(),
        ): List<RenderableDisplayTableEnum<T>> =
            enumValues<C>()
                .filter {
                    C::class.java.getField(it.name).annotations.any { annotation ->
                        annotation is ColumnTags &&
                            annotation.tag.intersect(require).isNotEmpty() &&
                            annotation.tag.intersect(exclude).isEmpty()
                    }
                }.filterIsInstance<RenderableDisplayTableEnum<T>>()
    }
}

enum class ColumnTag {
    LIST_VIEW,
    CSV_EXPORT,
    PERSONAL_DATA,
    OBSOLETE,
    VERSION_HISTORY_ONLY,
}

annotation class ColumnTags(
    vararg val tag: ColumnTag,
)

object DisplayTableCsvRenderer {
    const val SEPARATOR = ";"

    inline fun <reified E : Enum<E>, T> renderCsv(
        output: OutputStream,
        data: Iterable<T>,
        excludeTags: Set<ColumnTag> = emptySet(),
        noinline includeColumn: (RenderableDisplayTableEnum<T>) -> Boolean = { true },
    ) {
        val columns =
            RenderableDisplayTableEnum
                .getByTags<E, T>(setOf(ColumnTag.CSV_EXPORT), excludeTags)
                .filter(includeColumn)
        require(columns.isNotEmpty()) { "No columns with CSV_EXPORT tag found" }

        val header = columns.joinToString(SEPARATOR) { col -> escape(col.uiHeaderValue.toString()) }
        output.write("$header\n".toByteArray())

        data.forEach { row ->
            val csvRow = columns.joinToString(SEPARATOR) { col -> escape(col.getValue(row)) }
            output.write("$csvRow\n".toByteArray())
        }

        output.flush()
    }

    fun escape(value: String): String {
        val escapedValue = value.replace("\"", "\"\"")

        return if (escapedValue.contains(SEPARATOR) || escapedValue.contains("\"") || escapedValue.contains("\n")) {
            "\"$escapedValue\""
        } else {
            escapedValue
        }
    }
}
