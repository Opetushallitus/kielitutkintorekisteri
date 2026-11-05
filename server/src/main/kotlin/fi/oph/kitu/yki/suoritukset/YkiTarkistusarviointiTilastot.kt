package fi.oph.kitu.yki.suoritukset

import fi.oph.kitu.csv.toCsvFilenameSegment
import fi.oph.kitu.html.DisplayEnum
import fi.oph.kitu.html.table.tableFilterOptions
import fi.oph.kitu.html.table.valintaDescription
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.jdbc.getEnumOrNull
import fi.oph.kitu.jdbc.getTypedArrayOrNull
import fi.oph.kitu.webmvc.toUrlParam
import fi.oph.kitu.yki.Arviointitila
import fi.oph.kitu.yki.TutkinnonOsa
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import org.springframework.jdbc.core.RowMapper
import java.time.LocalDate

enum class Tarkistusarvioinnintila(
    val arviointitila: Arviointitila,
) : DisplayEnum {
    Hyvaksytty(Arviointitila.TARKISTUSARVIOINTI_HYVAKSYTTY),
    OdottaaHyvaksyntaa(Arviointitila.TARKISTUSARVIOITU),
    ;

    override fun displayText(): String =
        when (this) {
            Hyvaksytty -> UiText.Yki.Tilastot.tilaHyvaksytty
            OdottaaHyvaksyntaa -> UiText.Yki.Tilastot.tilaOdottaaHyvaksyntaa
        }.toString()
}

data class YkiTarkistusarviointiTilastoValinnat(
    var tila: List<Tarkistusarvioinnintila> = emptyList(),
    var aikaryhmittely: YkiSuoritusAikaryhmittely? = null,
    var ryhmittely: List<YkiTarkistusarviointiTilastoColumn> = emptyList(),
) {
    fun tilat(): List<Arviointitila> = tila.ifEmpty { Tarkistusarvioinnintila.entries }.map { it.arviointitila }

    fun valittuAikaryhmittely(): YkiSuoritusAikaryhmittely = aikaryhmittely ?: YkiSuoritusAikaryhmittely.Ei

    fun valitutMuutRyhmittelyt(): List<YkiTarkistusarviointiTilastoColumn> =
        YkiTarkistusarviointiTilastoColumn.muutRyhmittelyt.filter { it in ryhmittely }

    fun valittuRyhmittely(): List<YkiTarkistusarviointiTilastoColumn> =
        listOfNotNull(
            when (valittuAikaryhmittely()) {
                YkiSuoritusAikaryhmittely.Ei -> null
                YkiSuoritusAikaryhmittely.Tutkintopaiva -> YkiTarkistusarviointiTilastoColumn.Tutkintopaiva
                YkiSuoritusAikaryhmittely.Tutkintovuosi -> YkiTarkistusarviointiTilastoColumn.Tutkintovuosi
            },
        ) + valitutMuutRyhmittelyt()

    fun toMap(): Map<String, String?> =
        mapOf(
            "tila" to tila.toUrlParam(),
            "aikaryhmittely" to valittuAikaryhmittely().takeIf { it != YkiSuoritusAikaryhmittely.Ei }?.name,
            "ryhmittely" to
                valitutMuutRyhmittelyt().takeIf { it.isNotEmpty() }?.joinToString(",") { it.urlParam },
        )

    fun filterDescriptions(): List<String> = listOfNotNull(valintaDescription(UiText.Yki.Tilastot.tila, tila))

    fun csvFileName(params: YkiTarkistusarvioinnitParams): String =
        params.csvFileName(
            listOfNotNull(
                "yki_tarkistusarviointitilastot",
                tila.toCsvFilenameSegment { it.name.lowercase() },
                valittuRyhmittely()
                    .takeIf { it.isNotEmpty() }
                    ?.joinToString("-", prefix = "ryhmittely_") { it.urlParam },
            ).joinToString("_"),
            henkilotiedot = false,
        )
}

data class YkiTarkistusarviointiTilastoRyhma(
    val tutkintovuosi: Int? = null,
    val tutkintopaiva: LocalDate? = null,
    val tutkintokieli: Tutkintokieli? = null,
    val tutkintotaso: Tutkintotaso? = null,
)

data class YkiTarkistusarviointiTilastoPohja(
    val ryhma: YkiTarkistusarviointiTilastoRyhma,
    val tarkistetut: Set<TutkinnonOsa>,
    val muuttuneet: Set<TutkinnonOsa>,
    val lukumaara: Long,
) {
    companion object {
        val fromRow =
            RowMapper { rs, _ ->
                YkiTarkistusarviointiTilastoPohja(
                    ryhma =
                        YkiTarkistusarviointiTilastoRyhma(
                            tutkintovuosi = rs.getObject("tutkintovuosi", Integer::class.java)?.toInt(),
                            tutkintopaiva = rs.getObject("tutkintopaiva", LocalDate::class.java),
                            tutkintokieli = rs.getEnumOrNull<Tutkintokieli>("tutkintokieli"),
                            tutkintotaso = rs.getEnumOrNull<Tutkintotaso>("tutkintotaso"),
                        ),
                    tarkistetut =
                        rs
                            .getTypedArrayOrNull("tarkistusarvioidut_osakokeet") { TutkinnonOsa.valueOf(it) }
                            ?.toSet()
                            .orEmpty(),
                    muuttuneet =
                        rs
                            .getTypedArrayOrNull("arvosana_muuttui") { TutkinnonOsa.valueOf(it) }
                            ?.toSet()
                            .orEmpty(),
                    lukumaara = rs.getLong("lukumaara"),
                )
            }
    }
}

sealed interface YkiTarkistusarviointiTilastoKohde {
    val nimi: String

    data object Tarkistusarvioinnit : YkiTarkistusarviointiTilastoKohde {
        override val nimi: String get() =
            UiText.Yki.Tilastot.tarkistusarvioinnit
                .toString()
    }

    data object IlmanOsakoetietoa : YkiTarkistusarviointiTilastoKohde {
        override val nimi: String get() =
            UiText.Yki.Tilastot.ilmanOsakoetietoa
                .toString()
    }

    data object OsakokeetYhteensa : YkiTarkistusarviointiTilastoKohde {
        override val nimi: String get() =
            UiText.Yki.Tilastot.osakokeetYhteensa
                .toString()
    }

    data class Osakoe(
        val osa: TutkinnonOsa,
    ) : YkiTarkistusarviointiTilastoKohde {
        override val nimi: String get() = osa.viewText
    }
}

data class YkiTarkistusarviointiTilastoRivi(
    val ryhma: YkiTarkistusarviointiTilastoRyhma,
    val kohde: YkiTarkistusarviointiTilastoKohde,
    val yhteensa: Long,
    val muuttui: Long?,
    val eiMuuttunut: Long?,
)

private val RYHMIEN_JARJESTYS: Comparator<YkiTarkistusarviointiTilastoRyhma> =
    compareByDescending<YkiTarkistusarviointiTilastoRyhma> { it.tutkintovuosi }
        .thenByDescending { it.tutkintopaiva }
        .thenBy { it.tutkintokieli?.nimi?.toString() }
        .thenBy { it.tutkintotaso }

fun koostaTarkistusarviointiTilastot(
    pohjat: List<YkiTarkistusarviointiTilastoPohja>,
): List<YkiTarkistusarviointiTilastoRivi> =
    pohjat
        .groupBy { it.ryhma }
        .toSortedMap(RYHMIEN_JARJESTYS)
        .flatMap { (ryhma, ryhmanPohjat) -> ryhmanRivit(ryhma, ryhmanPohjat) }

private fun ryhmanRivit(
    ryhma: YkiTarkistusarviointiTilastoRyhma,
    pohjat: List<YkiTarkistusarviointiTilastoPohja>,
): List<YkiTarkistusarviointiTilastoRivi> {
    val (osakokeilla, ilmanOsakokeita) = pohjat.partition { it.tarkistetut.isNotEmpty() }
    val ilmanOsakoetietoa = ilmanOsakokeita.sumOf { it.lukumaara }

    fun osakoerivi(
        kohde: YkiTarkistusarviointiTilastoKohde,
        yhteensa: Long,
        muuttui: Long,
    ) = YkiTarkistusarviointiTilastoRivi(ryhma, kohde, yhteensa, muuttui, yhteensa - muuttui)

    val osakoeRivit =
        TutkinnonOsa.entries.map { osa ->
            osakoerivi(
                YkiTarkistusarviointiTilastoKohde.Osakoe(osa),
                osakokeilla.filter { osa in it.tarkistetut }.sumOf { it.lukumaara },
                osakokeilla.filter { osa in it.muuttuneet }.sumOf { it.lukumaara },
            )
        }

    return buildList {
        add(
            YkiTarkistusarviointiTilastoRivi(
                ryhma,
                YkiTarkistusarviointiTilastoKohde.Tarkistusarvioinnit,
                yhteensa = pohjat.sumOf { it.lukumaara },
                muuttui = osakokeilla.filter { it.muuttuneet.isNotEmpty() }.sumOf { it.lukumaara },
                eiMuuttunut = osakokeilla.filter { it.muuttuneet.isEmpty() }.sumOf { it.lukumaara },
            ),
        )
        if (ilmanOsakoetietoa > 0) {
            add(
                YkiTarkistusarviointiTilastoRivi(
                    ryhma,
                    YkiTarkistusarviointiTilastoKohde.IlmanOsakoetietoa,
                    yhteensa = ilmanOsakoetietoa,
                    muuttui = null,
                    eiMuuttunut = null,
                ),
            )
        }
        add(
            osakoerivi(
                YkiTarkistusarviointiTilastoKohde.OsakokeetYhteensa,
                osakoeRivit.sumOf { it.yhteensa },
                osakoeRivit.sumOf { it.muuttui ?: 0 },
            ),
        )
        val aina = tableFilterOptions<TutkinnonOsa>()
        addAll(
            osakoeRivit.filter { rivi ->
                rivi.yhteensa > 0 ||
                    (rivi.kohde as YkiTarkistusarviointiTilastoKohde.Osakoe).osa in aina
            },
        )
    }
}
