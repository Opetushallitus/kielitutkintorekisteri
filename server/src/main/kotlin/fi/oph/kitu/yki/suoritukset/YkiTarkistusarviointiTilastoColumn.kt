package fi.oph.kitu.yki.suoritukset

import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.html.table.ColumnTags
import fi.oph.kitu.html.table.RenderableDisplayTableEnum
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDate
import kotlinx.html.FlowContent
import kotlinx.html.strong

enum class YkiTarkistusarviointiTilastoColumn(
    override val entityName: String,
    override val urlParam: String,
    override val getValue: (value: YkiTarkistusarviointiTilastoRivi) -> String,
    val ryhmittelynSqlTyyppi: String? = null,
    val ryhmittelynSqlLauseke: String? = null,
    override val renderHtml: (FlowContent.(YkiTarkistusarviointiTilastoRivi) -> Unit)? = null,
) : RenderableDisplayTableEnum<YkiTarkistusarviointiTilastoRivi> {
    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Tutkintovuosi(
        entityName = "tutkintovuosi",
        urlParam = "tutkintovuosi",
        getValue = {
            it.ryhma.tutkintovuosi
                ?.toString()
                .orEmpty()
        },
        ryhmittelynSqlTyyppi = "int",
        ryhmittelynSqlLauseke = "EXTRACT(YEAR FROM tutkintopaiva)::int",
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Tutkintopaiva(
        entityName = "tutkintopaiva",
        urlParam = "tutkintopaiva",
        getValue = {
            it.ryhma.tutkintopaiva
                ?.finnishDate()
                .orEmpty()
        },
        ryhmittelynSqlTyyppi = "date",
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Tutkintokieli(
        entityName = "tutkintokieli",
        urlParam = "tutkintokieli",
        getValue = {
            it.ryhma.tutkintokieli
                ?.nimi
                ?.toString()
                .orEmpty()
        },
        ryhmittelynSqlTyyppi = "text",
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Tutkintotaso(
        entityName = "tutkintotaso",
        urlParam = "tutkintotaso",
        getValue = {
            it.ryhma.tutkintotaso
                ?.nimi
                ?.toString()
                .orEmpty()
        },
        ryhmittelynSqlTyyppi = "text",
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Kohde(
        entityName = "kohde",
        urlParam = "kohde",
        getValue = { it.kohde.nimi },
        renderHtml = { rivi ->
            if (rivi.kohde == YkiTarkistusarviointiTilastoKohde.Tarkistusarvioinnit) {
                strong { +rivi.kohde.nimi }
            } else {
                +rivi.kohde.nimi
            }
        },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Yhteensa(
        entityName = "yhteensa",
        urlParam = "yhteensa",
        getValue = { it.yhteensa.toString() },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    ArvosanaMuuttui(
        entityName = "muuttui",
        urlParam = "muuttui",
        getValue = { it.muuttui?.toString().orEmpty() },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    EiMuuttunut(
        entityName = "eiMuuttunut",
        urlParam = "eimuuttunut",
        getValue = { it.eiMuuttunut?.toString().orEmpty() },
    ),
    ;

    override val uiHeaderValue: LocalizedString
        get() =
            when (this) {
                Tutkintovuosi -> UiText.Yki.Sarake.tutkintovuosi
                Tutkintopaiva -> UiText.Yki.Sarake.tutkintopaiva
                Tutkintokieli -> UiText.Yki.Sarake.tutkintokieli
                Tutkintotaso -> UiText.Yki.Sarake.tutkintotaso
                Kohde -> UiText.Yki.Tilastot.kohde
                Yhteensa -> UiText.Yki.Tilastot.yhteensa
                ArvosanaMuuttui -> UiText.Yki.arvosanaMuuttui
                EiMuuttunut -> UiText.Yki.Tilastot.eiMuuttunut
            }

    val sqlLauseke: String get() = ryhmittelynSqlLauseke ?: entityName

    companion object {
        val ryhmittelyt: List<YkiTarkistusarviointiTilastoColumn>
            get() = entries.filter { it.ryhmittelynSqlTyyppi != null }

        val muutRyhmittelyt: List<YkiTarkistusarviointiTilastoColumn>
            get() = listOf(Tutkintokieli, Tutkintotaso)

        fun sarakkeet(ryhmittely: List<YkiTarkistusarviointiTilastoColumn>): List<YkiTarkistusarviointiTilastoColumn> =
            ryhmittely + listOf(Kohde, Yhteensa, ArvosanaMuuttui, EiMuuttunut)
    }
}
