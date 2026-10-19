package fi.oph.kitu.kotoutumiskoulutus.suoritukset.error

import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.html.table.ColumnTags
import fi.oph.kitu.html.table.RenderableDisplayTableEnum
import fi.oph.kitu.i18n.LocalizedString
import kotlinx.html.FlowContent
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

private val VIRHEEN_LUONTIAIKA: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX").withZone(ZoneOffset.UTC)

/**
 * Koton virhe-CSV:n sarakkeet. Otsikot ovat kielestä riippumattomia kenttänimiä, koska
 * tiedostoa käsitellään taulukko-ohjelmissa sarakkeiden nimillä.
 */
enum class KielitestiSuoritusErrorCsvColumn(
    header: String,
    override val getValue: (KielitestiSuoritusError) -> String,
) : RenderableDisplayTableEnum<KielitestiSuoritusError> {
    @ColumnTags(ColumnTag.CSV_EXPORT)
    VirheenLuontiaika("virheenLuontiaika", { VIRHEEN_LUONTIAIKA.format(it.virheenLuontiaika) }),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    SuorittajanOid("suorittajanOid", { it.suorittajanOid.orEmpty() }),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    Hetu("hetu", { it.hetu.orEmpty() }),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    Nimi("nimi", { it.nimi }),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    Etunimet("etunimet", { it.etunimet }),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    Sukunimi("sukunimi", { it.sukunimi }),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    Kutsumanimi("kutsumanimi", { it.kutsumanimi.orEmpty() }),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    SchoolOid("schoolOid", { it.schoolOid?.toString().orEmpty() }),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    TeacherEmail("teacherEmail", { it.teacherEmail.orEmpty() }),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    Viesti("viesti", { it.viesti }),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    Lisatietoja("lisatietoja", { it.lisatietoja.orEmpty() }),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    OnrLisatietoja("onrLisatietoja", { it.onrLisatietoja.orEmpty() }),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    OnrEtunimet("onrEtunimet", { it.onrEtunimet.orEmpty() }),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    OnrKutsumanimi("onrKutsumanimi", { it.onrKutsumanimi.orEmpty() }),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    OnrSukunimi("onrSukunimi", { it.onrSukunimi.orEmpty() }),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    VirheellinenKentta("virheellinenKentta", { it.virheellinenKentta.orEmpty() }),

    @ColumnTags(ColumnTag.CSV_EXPORT)
    VirheellinenArvo("virheellinenArvo", { it.virheellinenArvo.orEmpty() }),
    ;

    override val entityName: String = header
    override val urlParam: String = header
    override val uiHeaderValue: LocalizedString = LocalizedString(fi = header)
    override val renderHtml: ((FlowContent, KielitestiSuoritusError) -> Unit)? = null
}
