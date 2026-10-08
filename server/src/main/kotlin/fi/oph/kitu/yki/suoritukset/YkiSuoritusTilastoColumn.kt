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
    override val renderHtml: (FlowContent.(YkiSuoritusTilastoRivi) -> Unit)? = null,
) : RenderableDisplayTableEnum<YkiSuoritusTilastoRivi> {
    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Tutkintopaiva(
        entityName = "tutkintopaiva",
        urlParam = "tutkintopaiva",
        getValue = { it.tutkintopaiva.finnishDate() },
        comparator = compareBy { it.tutkintopaiva },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Tutkintokieli(
        entityName = "tutkintokieli",
        urlParam = "tutkintokieli",
        getValue = { it.tutkintokieli.nimi.toString() },
        comparator = compareBy { it.tutkintokieli.nimi.toString() },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Tutkintotaso(
        entityName = "tutkintotaso",
        urlParam = "tutkintotaso",
        getValue = { it.tutkintotaso.nimi.toString() },
        comparator = compareBy { it.tutkintotaso },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Arviointitila(
        entityName = "arviointitila",
        urlParam = "arviointitila",
        getValue = { it.arviointitila.viewText },
        comparator = compareBy { it.arviointitila },
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
                Tutkintopaiva -> UiText.Yki.Sarake.tutkintopaiva
                Tutkintokieli -> UiText.Yki.Sarake.tutkintokieli
                Tutkintotaso -> UiText.Yki.Sarake.tutkintotaso
                Arviointitila -> UiText.Yki.Sarake.arviointitila
                Lukumaara -> UiText.Yki.Tilastot.lukumaara
            }

    companion object {
        private val oletusjarjestys: Comparator<YkiSuoritusTilastoRivi> =
            Tutkintopaiva.comparator
                .reversed()
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
