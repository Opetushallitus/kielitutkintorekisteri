package fi.oph.kitu.yki.historia

import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.html.table.ColumnTags
import fi.oph.kitu.html.table.RenderableDisplayTableEnum
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import kotlinx.html.FlowContent

/**
 * Sarakkeiden esittelyjarjestys on CSV:n sarakejarjestys, ja se noudattaa lahdetiedoston
 * 30 sarakkeen jarjestysta - niin vienti palauttaa lahderivin sellaisenaan. HTML-taulussa
 * nakyy naista luettava osajoukko (LIST_VIEW).
 */
enum class YkiHistoriaSiirtymatonColumn(
    override val entityName: String,
    override val uiHeaderValue: LocalizedString,
    override val urlParam: String,
    override val getValue: (YkiHistoriaSiirtymatonEntity) -> String,
    override val renderHtml: (FlowContent.(YkiHistoriaSiirtymatonEntity) -> Unit)? = null,
) : RenderableDisplayTableEnum<YkiHistoriaSiirtymatonEntity> {
    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    SuorittajanOid(
        entityName = "suorittajan_oid",
        uiHeaderValue = UiText.Yki.Sarake.oppijanumero,
        urlParam = "suorittajanoid",
        getValue = { it.suorittajanOid.orEmpty() },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Hetu(
        entityName = "hetu",
        uiHeaderValue = UiText.Yki.Sarake.henkilotunnus,
        urlParam = "hetu",
        getValue = { it.hetu.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Sukupuoli(
        entityName = "sukupuoli",
        uiHeaderValue = UiText.Yki.Sarake.sukupuoli,
        urlParam = "sukupuoli",
        getValue = { it.sukupuoli.orEmpty() },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Sukunimi(
        entityName = "sukunimi",
        uiHeaderValue = UiText.Yki.Sarake.sukunimi,
        urlParam = "sukunimi",
        getValue = { it.sukunimi.orEmpty() },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Etunimet(
        entityName = "etunimet",
        uiHeaderValue = UiText.Yki.Sarake.etunimet,
        urlParam = "etunimet",
        getValue = { it.etunimet.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Kansalaisuus(
        entityName = "kansalaisuus",
        uiHeaderValue = UiText.Yki.Sarake.kansalaisuus,
        urlParam = "kansalaisuus",
        getValue = { it.kansalaisuus.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Katuosoite(
        entityName = "katuosoite",
        uiHeaderValue = UiText.Yki.Sarake.osoite,
        urlParam = "katuosoite",
        getValue = { it.katuosoite.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Postinumero(
        entityName = "postinumero",
        uiHeaderValue = UiText.Yki.Historia.postinumero,
        urlParam = "postinumero",
        getValue = { it.postinumero.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Postitoimipaikka(
        entityName = "postitoimipaikka",
        uiHeaderValue = UiText.Yki.Historia.postitoimipaikka,
        urlParam = "postitoimipaikka",
        getValue = { it.postitoimipaikka.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Email(
        entityName = "email",
        uiHeaderValue = UiText.Yki.Sarake.sahkoposti,
        urlParam = "email",
        getValue = { it.email.orEmpty() },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    SolkiId(
        entityName = "solki_id",
        uiHeaderValue = UiText.Yki.Sarake.solkiId,
        urlParam = "solkiid",
        getValue = { it.solkiId.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    LastModified(
        entityName = "last_modified",
        uiHeaderValue = UiText.Yki.Historia.muutosaikaleima,
        urlParam = "lastmodified",
        getValue = { it.lastModified.orEmpty() },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Tutkintopaiva(
        entityName = "tutkintopaiva",
        uiHeaderValue = UiText.Yki.Sarake.tutkintopaiva,
        urlParam = "tutkintopaiva",
        getValue = { it.tutkintopaiva.orEmpty() },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Tutkintokieli(
        entityName = "tutkintokieli",
        uiHeaderValue = UiText.Yki.Sarake.tutkintokieli,
        urlParam = "tutkintokieli",
        getValue = { it.tutkintokieli.orEmpty() },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Tutkintotaso(
        entityName = "tutkintotaso",
        uiHeaderValue = UiText.Yki.Sarake.tutkintotaso,
        urlParam = "tutkintotaso",
        getValue = { it.tutkintotaso.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    JarjestajanOid(
        entityName = "jarjestajan_oid",
        uiHeaderValue = UiText.Yki.Sarake.jarjestajanOid,
        urlParam = "jarjestajanoid",
        getValue = { it.jarjestajanOid.orEmpty() },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    JarjestajanNimi(
        entityName = "jarjestajan_nimi",
        uiHeaderValue = UiText.Yki.Sarake.jarjestajanNimi,
        urlParam = "jarjestajannimi",
        getValue = { it.jarjestajanNimi.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    Arviointipaiva(
        entityName = "arviointipaiva",
        uiHeaderValue = UiText.Yki.Sarake.arviointipaiva,
        urlParam = "arviointipaiva",
        getValue = { it.arviointipaiva.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    AsTy(
        entityName = "as_ty",
        uiHeaderValue = UiText.Yki.Sarake.tekstinYmmartaminen,
        urlParam = "asty",
        getValue = { it.asTy.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    AsKi(
        entityName = "as_ki",
        uiHeaderValue = UiText.Yki.Sarake.kirjoittaminen,
        urlParam = "aski",
        getValue = { it.asKi.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    AsRs(
        entityName = "as_rs",
        uiHeaderValue = UiText.Yki.Sarake.rakenteetJaSanasto,
        urlParam = "asrs",
        getValue = { it.asRs.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    AsPy(
        entityName = "as_py",
        uiHeaderValue = UiText.Yki.Sarake.puheenYmmartaminen,
        urlParam = "aspy",
        getValue = { it.asPy.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    AsPu(
        entityName = "as_pu",
        uiHeaderValue = UiText.Yki.Sarake.puhuminen,
        urlParam = "aspu",
        getValue = { it.asPu.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    AsYl(
        entityName = "as_yl",
        uiHeaderValue = UiText.Yki.Sarake.yleisarvosana,
        urlParam = "asyl",
        getValue = { it.asYl.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    TarkSaapumisPvm(
        entityName = "tark_saapumis_pvm",
        uiHeaderValue = UiText.Yki.Sarake.tarkistusarvioinninSaapumispaiva,
        urlParam = "tarksaapumispvm",
        getValue = { it.tarkSaapumisPvm.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    TarkAsiatunnus(
        entityName = "tark_asiatunnus",
        uiHeaderValue = UiText.Yki.Sarake.asiatunnus,
        urlParam = "tarkasiatunnus",
        getValue = { it.tarkAsiatunnus.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    TarkOsakokeet(
        entityName = "tark_osakokeet",
        uiHeaderValue = UiText.Yki.Sarake.tarkistusarvioidutOsakokeet,
        urlParam = "tarkosakokeet",
        getValue = { it.tarkOsakokeet.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    ArvosanaMuuttui(
        entityName = "arvosana_muuttui",
        uiHeaderValue = UiText.Yki.Sarake.arvosanaMuuttuiOsakokeet,
        urlParam = "arvosanamuuttui",
        getValue = { it.arvosanaMuuttui.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    Perustelu(
        entityName = "perustelu",
        uiHeaderValue = UiText.Yki.perustelu,
        urlParam = "perustelu",
        getValue = { it.perustelu.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    TarkKasittelyPvm(
        entityName = "tark_kasittely_pvm",
        uiHeaderValue = UiText.Yki.Sarake.tarkistusarvioinninKasittelypaiva,
        urlParam = "tarkkasittelypvm",
        getValue = { it.tarkKasittelyPvm.orEmpty() },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Syyluokka(
        entityName = "syyluokka",
        uiHeaderValue = UiText.Yki.Historia.syyluokka,
        urlParam = "syyluokka",
        getValue = { it.syyluokka.nimi.toString() },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Syy(
        entityName = "syy",
        uiHeaderValue = UiText.Yki.Historia.syy,
        urlParam = "syy",
        getValue = { it.syy },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    OidHaunSyy(
        entityName = "oid_haun_syy",
        uiHeaderValue = UiText.Yki.Historia.oidHaunSyy,
        urlParam = "oidhaunsyy",
        getValue = {
            it.oidHaunSyy ?: UiText.Yki.Historia.oidHakuaEiYritetty
                .toString()
        },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    RawRivi(
        entityName = "raw_rivi",
        uiHeaderValue = UiText.Yki.Historia.rikkinainenRivi,
        urlParam = "rawrivi",
        getValue = { it.rawRivi.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    Lahdetiedosto(
        entityName = "lahdetiedosto",
        uiHeaderValue = UiText.Yki.Historia.lahdetiedosto,
        urlParam = "lahdetiedosto",
        getValue = { it.lahdetiedosto },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Ladattu(
        entityName = "ladattu",
        uiHeaderValue = UiText.Yki.Historia.ladattu,
        urlParam = "ladattu",
        getValue = {
            it.ladattu
                ?.toLocalDate()
                ?.toString()
                .orEmpty()
        },
    ),
}
