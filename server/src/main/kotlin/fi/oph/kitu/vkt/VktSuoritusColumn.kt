package fi.oph.kitu.vkt
import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.html.table.ColumnTags
import fi.oph.kitu.html.table.RenderableDisplayTableEnum
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDate
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.webmvc.Links
import kotlinx.html.FlowContent
import kotlinx.html.a

enum class VktSuoritusColumn(
    override val entityName: String,
    override val uiHeaderValue: LocalizedString,
    override val urlParam: String,
    override val getValue: (value: VktSuoritusFlat) -> String,
    override val renderHtml: (FlowContent.(VktSuoritusFlat) -> Unit)? = null,
) : RenderableDisplayTableEnum<VktSuoritusFlat> {
    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.LIST_VIEW, ColumnTag.PERSONAL_DATA)
    SuoritusId(
        "suoritus_id",
        LocalizedString(fi = ""),
        "id",
        { it.suoritusId.toString() },
        {
            val url =
                Links.Vkt.ilmoittautuneenArviointi(
                    oppijanumero = it.suorittajanOid,
                    kieli = Koodisto.Tutkintokieli.valueOf(it.tutkintokieli),
                    taso = Koodisto.VktTaitotaso.valueOf(it.taitotaso),
                )

            a(href = url) { +UiText.Toiminto.nayta }
        },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    IlmoittautumisenTunniste(
        "ilmoittautumisId",
        UiText.Vkt.Sarake.ilmoittautumisenTunniste,
        "ilmoittautumisId",
        { it.ilmoittautumisenId },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Sukunimi(
        "sukunimi",
        UiText.Vkt.Sarake.sukunimi,
        "sukunimi",
        { it.sukunimi },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Etunimet(
        "etunimet",
        UiText.Vkt.Sarake.etunimet,
        "etunimet",
        { it.etunimet },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    SuorittajanOid(
        "suorittajanOid",
        UiText.Vkt.Sarake.oppijanumero,
        "suorittajanOid",
        { it.suorittajanOid },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Taitotaso(
        "taitotaso",
        UiText.Vkt.Sarake.taitotaso,
        "taitotaso",
        {
            Koodisto.VktTaitotaso
                .valueOf(it.taitotaso)
                .nimi
                .toString()
        },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Tutkintokieli(
        "tutkintokieli",
        UiText.Vkt.Sarake.tutkintokieli,
        "tutkintokieli",
        {
            Koodisto.Tutkintokieli
                .valueOf(it.tutkintokieli)
                .nimi
                .toString()
        },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Tutkintopaiva(
        "tutkintopaiva",
        UiText.Vkt.Sarake.tutkintopaiva,
        "tutkintopaiva",
        { it.tutkintopaiva.finnishDate() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    Suorituspaikkakunta(
        "suorituspaikkakunta",
        UiText.Vkt.Sarake.suorituspaikkakunta,
        "suorituspaikkakunta",
        { it.suorituspaikkakunta },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    SuorituksenVastaanottajanOid(
        "suorituksenVastaanottajanOid",
        UiText.Vkt.Sarake.vastaanottajanOid,
        "suorituksenVastaanottajanOid",
        { it.suorituksenVastaanottajanOid?.toString().orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    SuorituksenVastaanottaja(
        "suorituksenVastaanottaja",
        UiText.Vkt.Sarake.vastaanottaja,
        "suorituksenVastaanottaja",
        { it.suorituksenVastaanottaja.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    Puhuminen(
        "puhuminen",
        UiText.Vkt.Sarake.puhuminen,
        "puhuminen",
        { it.puhuminen.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    PuheenYmmartaminen(
        "puheenYmmartaminen",
        UiText.Vkt.Sarake.puheenYmmartaminen,
        "puheenYmmartaminen",
        { it.puheenYmmartaminen.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    Kirjoittaminen(
        "kirjoittaminen",
        UiText.Vkt.Sarake.kirjoittaminen,
        "kirjoittaminen",
        { it.kirjoittaminen.orEmpty() },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    TekstinYmmartaminen(
        "tekstinYmmartaminen",
        UiText.Vkt.Sarake.tekstinYmmartaminen,
        "tekstinYmmartaminen",
        { it.tekstinYmmartaminen.orEmpty() },
    ),
}
