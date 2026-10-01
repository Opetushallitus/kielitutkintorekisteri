package fi.oph.kitu.yki.suoritukset
import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.html.table.ColumnTags
import fi.oph.kitu.html.table.RenderableDisplayTableEnum
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDate
import fi.oph.kitu.i18n.finnishDateTime
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.webmvc.Links
import fi.oph.kitu.yki.TutkinnonOsa
import fi.oph.kitu.yki.Tutkintotaso
import kotlinx.html.FlowContent
import kotlinx.html.a
import kotlinx.html.span

/**
 * Enum class representing columns in YKI Suoritus.
 * The class also maps the column names between SQL data classes and UI.
 */
enum class YkiSuoritusColumn(
    override val entityName: String,
    override val uiHeaderValue: LocalizedString,
    override val urlParam: String,
    override val getValue: (value: YkiSuoritusEntity) -> String,
    override val renderHtml: (FlowContent.(YkiSuoritusEntity) -> Unit)? = null,
) : RenderableDisplayTableEnum<YkiSuoritusEntity> {
    @ColumnTags(ColumnTag.LIST_VIEW)
    Id(
        entityName = "id",
        uiHeaderValue = LocalizedString(fi = ""),
        urlParam = "id",
        getValue = { it.id?.toString().orEmpty() },
        renderHtml = {
            it.id?.let { id ->
                a(href = Links.Yki.suoritus(id)) { +UiText.Toiminto.nayta }
            }
        },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    SuorittajanOid(
        entityName = "suorittajan_oid",
        uiHeaderValue = UiText.Yki.Sarake.oppijanumero,
        urlParam = "suorittajanoid",
        getValue = { it.suorittajanOID?.toString().orEmpty() },
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
        uiHeaderValue = UiText.Yki.Sarake.etunimi,
        urlParam = "etunimet",
        getValue = { it.etunimet },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    Sukupuoli(
        entityName = "sukupuoli",
        uiHeaderValue = UiText.Yki.Sarake.sukupuoli,
        urlParam = "sukupuoli",
        getValue = { it.sukupuoli.name },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Hetu(
        entityName = "hetu",
        uiHeaderValue = UiText.Yki.Sarake.henkilotunnus,
        urlParam = "hetu",
        getValue = { it.hetu.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    Kansalaisuus(
        entityName = "kansalaisuus",
        uiHeaderValue = UiText.Yki.Sarake.kansalaisuus,
        urlParam = "kansalaisuus",
        getValue = { it.kansalaisuus },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Katuosoite(
        entityName = "katuosoite",
        uiHeaderValue = UiText.Yki.Sarake.osoite,
        urlParam = "katuosoite",
        getValue = { osoite(it.katuosoite, it.postinumero, it.postitoimipaikka, it.maa) },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Email(
        entityName = "email",
        uiHeaderValue = UiText.Yki.Sarake.sahkoposti,
        urlParam = "email",
        getValue = { it.email.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.LIST_VIEW)
    Tutkintopaiva(
        entityName = "tutkintopaiva",
        uiHeaderValue = UiText.Yki.Sarake.tutkintopaiva,
        urlParam = "tutkintopaiva",
        getValue = { it.tutkintopaiva.finnishDate() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.LIST_VIEW)
    Tutkintokieli(
        entityName = "tutkintokieli",
        uiHeaderValue = UiText.Yki.Sarake.tutkintokieli,
        urlParam = "tutkintokieli",
        getValue = { it.tutkintokieli.name },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.LIST_VIEW)
    Tutkintotaso(
        entityName = "tutkintotaso",
        uiHeaderValue = UiText.Yki.Sarake.tutkintotaso,
        urlParam = "tutkintotaso",
        getValue = { it.tutkintotaso.name },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    JarjestajanTunnusOid(
        entityName = "jarjestajan_tunnus_oid",
        uiHeaderValue = UiText.Yki.Sarake.jarjestajanOid,
        urlParam = "jarjestajantunnusoid",
        getValue = { it.jarjestajanTunnusOid.toString() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    JarjestajanNimi(
        entityName = "jarjestajan_nimi",
        uiHeaderValue = UiText.Yki.Sarake.jarjestajanNimi,
        urlParam = "jarjestajannimi",
        getValue = { it.jarjestajanNimi },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Arviointitila(
        entityName = "arviointitila",
        uiHeaderValue = UiText.Yki.Sarake.arviointitila,
        urlParam = "arviointitila",
        getValue = { it.arviointitila.viewText },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    Arviointipaiva(
        entityName = "arviointipaiva",
        uiHeaderValue = UiText.Yki.Sarake.arviointipaiva,
        urlParam = "arviointipaiva",
        getValue = { it.arviointipaiva?.finnishDate().orEmpty() },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    ReceivedAt(
        entityName = "received_at",
        uiHeaderValue = UiText.Yki.Sarake.rekisteriintuontiaika,
        urlParam = "receivedat",
        getValue = { it.receivedAt.finnishDateTime() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    TekstinYmmartaminen(
        entityName = "tekstin_ymmartaminen",
        uiHeaderValue = UiText.Yki.Sarake.tekstinYmmartaminen,
        urlParam = "tekstinymmartaminen",
        getValue = { ykiArvosanaText(it.tekstinYmmartaminen, it.tutkintotaso) },
        renderHtml = { ykiArvosana(it.tekstinYmmartaminen, it.tutkintotaso) },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    Kirjoittaminen(
        entityName = "kirjoittaminen",
        uiHeaderValue = UiText.Yki.Sarake.kirjoittaminen,
        urlParam = "kirjoittaminen",
        getValue = { ykiArvosanaText(it.kirjoittaminen, it.tutkintotaso) },
        renderHtml = { ykiArvosana(it.kirjoittaminen, it.tutkintotaso) },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    PuheenYmmartamainen(
        entityName = "puheen_ymmartaminen",
        uiHeaderValue = UiText.Yki.Sarake.puheenYmmartaminen,
        urlParam = "puheenymmartamainen",
        getValue = { ykiArvosanaText(it.puheenYmmartaminen, it.tutkintotaso) },
        renderHtml = { ykiArvosana(it.puheenYmmartaminen, it.tutkintotaso) },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    Puhuminen(
        entityName = "puhuminen",
        uiHeaderValue = UiText.Yki.Sarake.puhuminen,
        urlParam = "puhuminen",
        getValue = { ykiArvosanaText(it.puhuminen, it.tutkintotaso) },
        renderHtml = { ykiArvosana(it.puhuminen, it.tutkintotaso) },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.OBSOLETE)
    RakenteetJaSanasto(
        entityName = "rakenteet_ja_sanasto",
        uiHeaderValue = UiText.Yki.Sarake.rakenteetJaSanasto,
        urlParam = "rakenteetjasanasto",
        getValue = { ykiArvosanaText(it.rakenteetJaSanasto, it.tutkintotaso) },
        renderHtml = { ykiArvosana(it.rakenteetJaSanasto, it.tutkintotaso) },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.OBSOLETE)
    Yleisarvosana(
        entityName = "yleisarvosana",
        uiHeaderValue = UiText.Yki.Sarake.yleisarvosana,
        urlParam = "yleisarvosana",
        getValue = { ykiArvosanaText(it.yleisarvosana, it.tutkintotaso) },
        renderHtml = { ykiArvosana(it.yleisarvosana, it.tutkintotaso) },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    Todistuskieli(
        entityName = "todistuskieli",
        uiHeaderValue = UiText.Yki.Sarake.todistuskieli,
        urlParam = "todistuskieli",
        getValue = { it.todistuskieli?.name.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    TarkistusarvioinninSaapumisPvm(
        entityName = "tarkistusarvioinnin_saapumis_pvm",
        uiHeaderValue = UiText.Yki.Sarake.tarkistusarvioinninSaapumispaiva,
        urlParam = "tarkistusarvioinninsaapumispvm",
        getValue = { it.tarkistusarvioinninSaapumisPvm?.finnishDate().orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    TarkistusarvioinninKasittelyPvm(
        entityName = "tarkistusarvioinnin_kasittely_pvm",
        uiHeaderValue = UiText.Yki.Sarake.tarkistusarvioinninKasittelypaiva,
        urlParam = "tarkistusarvioinninkasittelypvm",
        getValue = { it.tarkistusarvioinninKasittelyPvm?.finnishDate().orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    TarkistusarviointiHyvaksyttyPvm(
        entityName = "tarkistusarviointi_hyvaksytty_pvm",
        uiHeaderValue = UiText.Yki.Sarake.tarkistusarviointiHyvaksytty,
        urlParam = "tarkistusarviointihyvaksyttypvm",
        getValue = { it.tarkistusarviointiHyvaksyttyPvm?.finnishDate().orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    TarkistusarvioinninAsiatunnus(
        entityName = "tarkistusarvioinnin_asiatunnus",
        uiHeaderValue = UiText.Yki.Sarake.asiatunnus,
        urlParam = "tarkistusarvioinninasiatunnus",
        getValue = { it.tarkistusarvioinninAsiatunnus.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    TarkistusarvioidutOsakokeet(
        entityName = "tarkistusarvioidut_osakokeet",
        uiHeaderValue = UiText.Yki.Sarake.tarkistusarvioidutOsakokeet,
        urlParam = "tarkistusarvioidutosakokeet",
        getValue = { osakoeLista(it.tarkistusarvioidutOsakokeet) },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    ArvosanaMuuttui(
        entityName = "arvosana_muuttui",
        uiHeaderValue = UiText.Yki.Sarake.arvosanaMuuttuiOsakokeet,
        urlParam = "arvosanamuuttui",
        getValue = { osakoeLista(it.arvosanaMuuttui) },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    TarkistusarvioinninPerustelu(
        entityName = "perustelu",
        uiHeaderValue = UiText.Yki.perustelu,
        urlParam = "tarkistusarvioinninperustelu",
        getValue = { it.perustelu.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    ArviointitilaLahetetty(
        entityName = "arviointitila_lahetetty",
        uiHeaderValue = UiText.Yki.Sarake.tilaLahetetty,
        urlParam = "arviointitilalahetetty",
        getValue = { it.arviointitilanLahetysvirhe ?: it.arviointitilaLahetetty?.toString() ?: "" },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    OpiskeluoikeusOid(
        entityName = "opiskeluoikeus_oid",
        uiHeaderValue = UiText.Yki.Sarake.opiskeluoikeusOid,
        urlParam = "opiskeluoikeus_oid",
        getValue = { it.koskiOpiskeluoikeus?.toString().orEmpty() },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA, ColumnTag.VERSION_HISTORY_ONLY)
    SolkiId(
        entityName = "solki_id",
        uiHeaderValue = UiText.Yki.Sarake.solkiTunniste,
        urlParam = "solkiId",
        getValue = { it.solkiId.toString() },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.VERSION_HISTORY_ONLY)
    Version(
        entityName = "last_modified",
        uiHeaderValue = UiText.Yki.Sarake.versio,
        urlParam = "version",
        getValue = { it.id?.toString().orEmpty() },
        renderHtml = { finnishDateTime(it.lastModified) },
    ),
}

fun osoite(
    katuosoite: String,
    postinumero: String,
    postitoimipaikka: String,
    maa: String?,
) = if (maa ==
    null
) {
    "$katuosoite, $postinumero $postitoimipaikka"
} else {
    "$katuosoite, $postinumero $postitoimipaikka, $maa"
}

fun osakoeLista(osakokeet: Set<TutkinnonOsa>?): String =
    osakokeet
        .orEmpty()
        .sortedBy { it.ordinal }
        .joinToString(", ") { it.viewText }

fun ykiArvosanaText(
    arvosana: Int?,
    taso: Tutkintotaso,
): String =
    arvosana?.let {
        Koodisto.YkiArvosana
            .of(arvosana, taso)
            .getOrNull()
            ?.viewText
    } ?: ""

fun FlowContent.ykiArvosana(
    arvosana: Int?,
    taso: Tutkintotaso,
) = arvosana?.let {
    Koodisto.YkiArvosana.of(arvosana, taso).fold(
        ifLeft = {
            span(classes = "invalid-value") {
                +arvosana.toString()
            }
        },
        ifRight = { +it.viewText },
    )
}
