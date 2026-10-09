package fi.oph.kitu.yki.suoritukset

import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.html.table.ColumnTags
import fi.oph.kitu.html.table.RenderableDisplayTableEnum
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDate
import fi.oph.kitu.jdbc.SortDirection
import kotlinx.html.FlowContent

enum class YkiSuoritusTilastoColumn(
    override val entityName: String,
    override val urlParam: String,
    override val getValue: (value: YkiSuoritusTilastoRivi) -> String,
    val comparator: Comparator<YkiSuoritusTilastoRivi>,
    val ryhmittelynSqlTyyppi: String? = null,
    val ryhmittelynSqlLauseke: String? = null,
    override val renderHtml: (FlowContent.(YkiSuoritusTilastoRivi) -> Unit)? = null,
) : RenderableDisplayTableEnum<YkiSuoritusTilastoRivi> {
    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Tutkintovuosi(
        entityName = "tutkintovuosi",
        urlParam = "tutkintovuosi",
        getValue = { it.tutkintovuosi?.toString().orEmpty() },
        comparator = compareBy { it.tutkintovuosi },
        ryhmittelynSqlTyyppi = "int",
        ryhmittelynSqlLauseke = "EXTRACT(YEAR FROM tutkintopaiva)::int",
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Tutkintopaiva(
        entityName = "tutkintopaiva",
        urlParam = "tutkintopaiva",
        getValue = { it.tutkintopaiva?.finnishDate().orEmpty() },
        comparator = compareBy { it.tutkintopaiva },
        ryhmittelynSqlTyyppi = "date",
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Tutkintokieli(
        entityName = "tutkintokieli",
        urlParam = "tutkintokieli",
        getValue = {
            it.tutkintokieli
                ?.nimi
                ?.toString()
                .orEmpty()
        },
        comparator = compareBy { it.tutkintokieli?.nimi?.toString() },
        ryhmittelynSqlTyyppi = "text",
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Tutkintotaso(
        entityName = "tutkintotaso",
        urlParam = "tutkintotaso",
        getValue = {
            it.tutkintotaso
                ?.nimi
                ?.toString()
                .orEmpty()
        },
        comparator = compareBy { it.tutkintotaso },
        ryhmittelynSqlTyyppi = "text",
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Arviointitila(
        entityName = "arviointitila",
        urlParam = "arviointitila",
        getValue = { it.arviointitila?.viewText.orEmpty() },
        comparator = compareBy { it.arviointitila },
        ryhmittelynSqlTyyppi = "text",
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Lukumaara(
        entityName = "lukumaara",
        urlParam = "lukumaara",
        getValue = { it.lukumaara.toString() },
        comparator = compareBy { it.lukumaara },
    ),
    ;

    override val uiHeaderValue: LocalizedString
        get() =
            when (this) {
                Tutkintovuosi -> UiText.Yki.Sarake.tutkintovuosi
                Tutkintopaiva -> UiText.Yki.Sarake.tutkintopaiva
                Tutkintokieli -> UiText.Yki.Sarake.tutkintokieli
                Tutkintotaso -> UiText.Yki.Sarake.tutkintotaso
                Arviointitila -> UiText.Yki.Sarake.arviointitila
                Lukumaara -> UiText.Yki.Tilastot.lukumaara
            }

    val ryhmiteltava: Boolean get() = ryhmittelynSqlTyyppi != null

    val sqlLauseke: String get() = ryhmittelynSqlLauseke ?: entityName

    companion object {
        val ryhmittelyt: List<YkiSuoritusTilastoColumn> get() = entries.filter { it.ryhmiteltava }

        val aikasarakkeet: List<YkiSuoritusTilastoColumn> get() = listOf(Tutkintovuosi, Tutkintopaiva)

        val muutRyhmittelyt: List<YkiSuoritusTilastoColumn> get() = ryhmittelyt - aikasarakkeet.toSet()

        val oletusryhmittely: List<YkiSuoritusTilastoColumn> get() = listOf(Tutkintopaiva) + muutRyhmittelyt

        fun sarakkeet(ryhmittely: List<YkiSuoritusTilastoColumn>): List<YkiSuoritusTilastoColumn> =
            ryhmittely + Lukumaara

        private val oletusjarjestys: Comparator<YkiSuoritusTilastoRivi> =
            Tutkintovuosi.comparator
                .reversed()
                .then(Tutkintopaiva.comparator.reversed())
                .then(Tutkintokieli.comparator)
                .then(Tutkintotaso.comparator)
                .then(Arviointitila.comparator)

        fun jarjesta(
            rivit: List<YkiSuoritusTilastoRivi>,
            column: YkiSuoritusTilastoColumn,
            direction: SortDirection,
        ): List<YkiSuoritusTilastoRivi> =
            rivit.sortedWith(
                (if (direction == SortDirection.ASC) column.comparator else column.comparator.reversed())
                    .then(oletusjarjestys),
            )
    }
}
