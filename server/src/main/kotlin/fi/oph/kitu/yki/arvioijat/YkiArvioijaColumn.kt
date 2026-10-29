package fi.oph.kitu.yki.arvioijat

import fi.oph.kitu.html.copyable
import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.html.table.ColumnTags
import fi.oph.kitu.html.table.RenderableDisplayTableEnum
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDate
import fi.oph.kitu.i18n.finnishDateTime
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.webmvc.Links
import kotlinx.html.FlowContent
import kotlinx.html.a

enum class YkiArvioijaColumn(
    override val entityName: String,
    override val uiHeaderValue: LocalizedString,
    override val urlParam: String,
    override val getValue: (YkiArvioijaListRow) -> String,
    override val renderHtml: (FlowContent.(YkiArvioijaListRow) -> Unit)? = null,
) : RenderableDisplayTableEnum<YkiArvioijaListRow> {
    @ColumnTags(ColumnTag.LIST_VIEW)
    Linkki(
        entityName = "arvioija_id",
        uiHeaderValue = LocalizedString(fi = ""),
        urlParam = "id",
        getValue = { it.arvioijaId.toString() },
        renderHtml = {
            a(href = Links.Yki.arvioija(it.arvioijaId)) { +UiText.Toiminto.nayta }
        },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Oppijanumero(
        entityName = "arvioija_oid",
        uiHeaderValue = UiText.Yki.Sarake.oppijanumero,
        urlParam = "oppijanumero",
        getValue = { it.arvioijaOid.toString() },
        renderHtml = { copyable(it.arvioijaOid.toString()) },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Sukunimi(
        entityName = "sukunimi",
        uiHeaderValue = UiText.Yki.Sarake.sukunimi,
        urlParam = "sukunimi",
        getValue = { it.sukunimi },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Etunimet(
        entityName = "etunimet",
        uiHeaderValue = UiText.Yki.Sarake.etunimet,
        urlParam = "etunimet",
        getValue = { it.etunimet },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Email(
        entityName = "sahkopostiosoite",
        uiHeaderValue = UiText.Yki.Sarake.sahkoposti,
        urlParam = "email",
        getValue = { it.sahkopostiosoite.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Katuosoite(
        entityName = "katuosoite",
        uiHeaderValue = UiText.Yki.Sarake.osoite,
        urlParam = "katuosoite",
        getValue = { it.osoite },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Kieli(
        entityName = "kieli",
        uiHeaderValue = UiText.Yki.Sarake.kieli,
        urlParam = "kieli",
        getValue = { it.kieli.nimi.toString() },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Tasot(
        entityName = "tasot",
        uiHeaderValue = UiText.Yki.Sarake.tasot,
        urlParam = "tasot",
        getValue = { row -> row.tasot.sortedBy { it.ordinal }.joinToString(", ") { it.nimi.toString() } },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Tila(
        entityName = "tila",
        uiHeaderValue = UiText.Yki.Sarake.tila,
        urlParam = "tila",
        getValue = { it.tila.nimi.toString() },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    KaudenAlkupaiva(
        entityName = "kauden_alkupaiva",
        uiHeaderValue = UiText.Yki.Sarake.kaudenAlkupaiva,
        urlParam = "kaudenalkupaiva",
        getValue = { it.kaudenAlkupaiva?.toString().orEmpty() },
        renderHtml = { row -> row.kaudenAlkupaiva?.let { finnishDate(it) } },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    KaudenPaattymispaiva(
        entityName = "kauden_paattymispaiva",
        uiHeaderValue = UiText.Yki.Sarake.kaudenPaattymispaiva,
        urlParam = "kaudenpaattymispaiva",
        getValue = { it.kaudenPaattymispaiva?.toString().orEmpty() },
        renderHtml = { row -> row.kaudenPaattymispaiva?.let { finnishDate(it) } },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    Jatkorekisterointi(
        entityName = "jatkorekisterointi",
        uiHeaderValue = UiText.Yki.Sarake.jatkorekisterointi,
        urlParam = "jatkorekisterointi",
        getValue = { it.jatkorekisterointi.toString() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    EnsimmainenRekisterointipaiva(
        entityName = "ensimmainen_rekisterointipaiva",
        uiHeaderValue = UiText.Yki.Sarake.ensimmainenRekisterointipaiva,
        urlParam = "ensimmainenrekisterointipaiva",
        getValue = { it.ensimmainenRekisterointipaiva.toString() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    AshaNumero(
        entityName = "asha_numero",
        uiHeaderValue = UiText.Yki.Sarake.ashaNumero,
        urlParam = "ashanumero",
        getValue = { it.ashaNumero.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    SolkiTila(
        entityName = "solkiin_lahetetty",
        uiHeaderValue = UiText.Yki.Sarake.solkiTila,
        urlParam = "solkitila",
        getValue = { it.solkiTila.toString() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    Muokattu(
        entityName = "muokattu",
        uiHeaderValue = UiText.Yki.Sarake.muokattu,
        urlParam = "muokattu",
        getValue = { it.muokattu?.toString().orEmpty() },
        renderHtml = { row -> row.muokattu?.toInstant()?.let { finnishDateTime(it) } },
    ),
}
