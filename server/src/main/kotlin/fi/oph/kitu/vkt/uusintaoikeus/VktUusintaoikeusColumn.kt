package fi.oph.kitu.vkt.uusintaoikeus

import fi.oph.kitu.html.copyable
import fi.oph.kitu.html.table.ColumnTag
import fi.oph.kitu.html.table.ColumnTags
import fi.oph.kitu.html.table.RenderableDisplayTableEnum
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDate
import fi.oph.kitu.jdbc.SortDirection
import fi.oph.kitu.webmvc.Links
import kotlinx.html.FlowContent
import kotlinx.html.a

enum class VktUusintaoikeusColumn(
    override val entityName: String,
    override val urlParam: String,
    override val getValue: (VktUusintaoikeus) -> String,
    val comparator: Comparator<VktUusintaoikeus>,
    override val renderHtml: (FlowContent.(VktUusintaoikeus) -> Unit)? = null,
) : RenderableDisplayTableEnum<VktUusintaoikeus> {
    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Oppijanumero(
        entityName = "suorittajanOid",
        urlParam = "oppijanumero",
        getValue = { it.suorittajanOid },
        comparator = compareBy { it.suorittajanOid },
        renderHtml = {
            copyable(it.suorittajanOid) {
                a(href = Links.Vkt.ilmoittautuneenArviointi(it.suorittajanOid, it.tutkintokieli, it.taitotaso)) {
                    +it.suorittajanOid
                }
            }
        },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Sukunimi(
        entityName = "sukunimi",
        urlParam = "sukunimi",
        getValue = { it.sukunimi },
        comparator = compareBy(String.CASE_INSENSITIVE_ORDER) { it.sukunimi },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Etunimet(
        entityName = "etunimet",
        urlParam = "etunimet",
        getValue = { it.etunimet },
        comparator = compareBy(String.CASE_INSENSITIVE_ORDER) { it.etunimet },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Tutkintokieli(
        entityName = "tutkintokieli",
        urlParam = "tutkintokieli",
        getValue = { it.tutkintokieli.nimi.toString() },
        comparator = compareBy { it.tutkintokieli },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Taitotaso(
        entityName = "taitotaso",
        urlParam = "taitotaso",
        getValue = { it.taitotaso.nimi.toString() },
        comparator = compareBy { it.taitotaso },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Osakoe(
        entityName = "osakoe",
        urlParam = "osakoe",
        getValue = { it.osakoeNimi },
        comparator = compareBy { it.osakoeNimi },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Tutkinnot(
        entityName = "tutkinnot",
        urlParam = "tutkinnot",
        getValue = { it.tutkinnotNimet },
        comparator = compareBy { it.tutkinnotNimet },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    Hylkaamispaatos(
        entityName = "hylkaamispaatos",
        urlParam = "hylkaamispaatos",
        getValue = { it.hylkaamispaatos.finnishDate() },
        comparator = compareBy { it.hylkaamispaatos },
    ),

    @ColumnTags(ColumnTag.LIST_VIEW, ColumnTag.CSV_EXPORT)
    OikeusPaattyy(
        entityName = "oikeusPaattyy",
        urlParam = "oikeusPaattyy",
        getValue = { it.oikeusPaattyy.finnishDate() },
        comparator = compareBy { it.oikeusPaattyy },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Puhelinnumero(
        entityName = "puhelinnumero",
        urlParam = "puhelinnumero",
        getValue = { it.yhteystiedot?.puhelinnumero.orEmpty() },
        comparator = compareBy { it.yhteystiedot?.puhelinnumero },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Sahkoposti(
        entityName = "sahkoposti",
        urlParam = "sahkoposti",
        getValue = { it.yhteystiedot?.sahkoposti.orEmpty() },
        comparator = compareBy { it.yhteystiedot?.sahkoposti },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Katuosoite(
        entityName = "katuosoite",
        urlParam = "katuosoite",
        getValue = { it.yhteystiedot?.katuosoite.orEmpty() },
        comparator = compareBy { it.yhteystiedot?.katuosoite },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Postinumero(
        entityName = "postinumero",
        urlParam = "postinumero",
        getValue = { it.yhteystiedot?.postinumero.orEmpty() },
        comparator = compareBy { it.yhteystiedot?.postinumero },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Postitoimipaikka(
        entityName = "postitoimipaikka",
        urlParam = "postitoimipaikka",
        getValue = { it.yhteystiedot?.postitoimipaikka.orEmpty() },
        comparator = compareBy { it.yhteystiedot?.postitoimipaikka },
    ),

    @ColumnTags(ColumnTag.CSV_EXPORT, ColumnTag.PERSONAL_DATA)
    Maa(
        entityName = "maa",
        urlParam = "maa",
        getValue = { it.yhteystiedot?.maa.orEmpty() },
        comparator = compareBy { it.yhteystiedot?.maa },
    ),
    ;

    override val uiHeaderValue: LocalizedString
        get() =
            when (this) {
                Oppijanumero -> UiText.Vkt.Sarake.oppijanumero
                Sukunimi -> UiText.Vkt.Sarake.sukunimi
                Etunimet -> UiText.Vkt.Sarake.etunimet
                Tutkintokieli -> UiText.Vkt.Sarake.tutkintokieli
                Taitotaso -> UiText.Vkt.Sarake.taitotaso
                Osakoe -> UiText.Vkt.Sarake.uusittavaOsakoe
                Tutkinnot -> UiText.Vkt.tutkinnot
                Hylkaamispaatos -> UiText.Vkt.Sarake.hylkaamispaatos
                OikeusPaattyy -> UiText.Vkt.Sarake.oikeusPaattyy
                Puhelinnumero -> UiText.Vkt.Sarake.puhelinnumero
                Sahkoposti -> UiText.Vkt.Sarake.sahkoposti
                Katuosoite -> UiText.Vkt.Sarake.katuosoite
                Postinumero -> UiText.Vkt.Sarake.postinumero
                Postitoimipaikka -> UiText.Vkt.Sarake.postitoimipaikka
                Maa -> UiText.Vkt.Sarake.maa
            }

    companion object {
        private val oletusjarjestys: Comparator<VktUusintaoikeus> =
            Sukunimi.comparator
                .then(Etunimet.comparator)
                .then(Oppijanumero.comparator)
                .then(Tutkintokieli.comparator)
                .then(Taitotaso.comparator)
                .then(compareBy { it.osakoe })

        fun jarjesta(
            rivit: List<VktUusintaoikeus>,
            column: VktUusintaoikeusColumn,
            direction: SortDirection,
        ): List<VktUusintaoikeus> =
            rivit.sortedWith(
                (if (direction == SortDirection.ASC) column.comparator else column.comparator.reversed())
                    .then(oletusjarjestys),
            )
    }
}
