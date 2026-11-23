package fi.oph.kitu.tietosisalto

import fi.oph.kitu.koski.KoskiJacksonConfig
import fi.oph.kitu.koski.KoskiRequest.Opiskeluoikeus.KielitutkintoSuoritus.Vahvistus
import fi.oph.kitu.koski.Osasuoritus
import fi.oph.kitu.oid.Oid
import org.junit.jupiter.api.Test
import tools.jackson.databind.JsonNode
import java.nio.file.Files
import java.nio.file.Path
import java.time.LocalDate
import java.time.OffsetDateTime
import kotlin.io.path.readText
import kotlin.io.path.writeText
import kotlin.test.assertEquals
import kotlin.test.fail

class LahtevatTiedotDokumenttiTest {
    private val mapper = KoskiJacksonConfig().koskiObjectMapper()

    private val repo: Path =
        generateSequence(Path.of("").toAbsolutePath()) { it.parent }
            .first { Files.isDirectory(it.resolve("docs/technical")) && Files.exists(it.resolve("server/pom.xml")) }

    private val sivu = repo.resolve("docs/technical/lahtevat-tiedot.md")

    private val kentatVirroittain = Tietovirrat.kaikki.associateWith { kentat(it, mapper).flatMap(Kentta::kaikki) }

    @Test
    fun `sivu vastaa koodia`() {
        val tuotettu = renderLahtevatTiedot(Tietovirrat.kaikki, mapper)
        if (System.getProperty("kitu.dokumentaatio.paivita") == "true") {
            sivu.writeText(tuotettu)
            return
        }
        assertEquals(
            tuotettu,
            sivu.readText().replace("\r\n", "\n"),
            "docs/technical/lahtevat-tiedot.md ei vastaa koodia. Päivitä sivu hakemistossa server/: $PAIVITYSKOMENTO",
        )
    }

    @Test
    fun `jokaisella lahetettavalla tiedolla on kuvaus`() {
        val kuvaamattomat =
            kentatVirroittain.values
                .flatten()
                .filter { it.kuvaus.isNullOrBlank() }
                .map { "${it.luokka.simpleName}.${it.nimi}" }
                .distinct()
        assertEquals(emptyList(), kuvaamattomat, "Lisää näille kentille @Kuvaus")
    }

    @Test
    fun `tarkennukset osoittavat olemassa oleviin kenttiin`() {
        val tuntemattomat =
            kentatVirroittain.flatMap { (virta, kentat) ->
                (virta.tarkennukset.keys - kentat.map { it.polku }.toSet()).map { "${virta.ankkuri}: $it" }
            }
        assertEquals(emptyList(), tuntemattomat)
    }

    @Test
    fun `jokainen alatyyppi kuuluu johonkin tietovirtaan`() {
        val dokumentoidut =
            kentatVirroittain.values
                .flatten()
                .map { it.luokka }
                .toSet()
        val puuttuvat = (Vahvistus::class.sealedSubclasses + Osasuoritus::class.sealedSubclasses) - dokumentoidut
        assertEquals(emptyList(), puuttuvat, "Lisää alatyyppi johonkin tietovirtaan luokassa Tietovirrat")
    }

    @Test
    fun `tietovirtojen ankkurit ovat yksilollisia`() {
        val ankkurit = Tietovirrat.kaikki.map { it.ankkuri }
        assertEquals(ankkurit.distinct(), ankkurit)
    }

    @Test
    fun `esimerkkisanomat vastaavat dokumentoitua rakennetta`() {
        val virheet =
            Tietovirrat.kaikki.flatMap { virta ->
                val sanoma = mapper.valueToTree<JsonNode>(Esimerkit.sanoma(virta))
                tarkista(kentat(virta, mapper), sanoma, "").map { "${virta.ankkuri}: $it" }
            }
        if (virheet.isNotEmpty()) fail(virheet.joinToString("\n"))
    }

    @Test
    fun `integraatiot-sivun linkit osoittavat olemassa oleviin tietovirtoihin`() {
        val linkit =
            Regex("""lahtevat-tiedot\.md#([a-z0-9-]+)""")
                .findAll(repo.resolve("docs/technical/integraatiot.md").readText())
                .map { it.groupValues[1] }
                .toSet()
        assertEquals(Tietovirrat.kaikki.map { it.ankkuri }.toSet(), linkit)
    }

    private fun tarkista(
        kentat: List<Kentta>,
        solmu: JsonNode,
        etuliite: String,
    ): List<String> {
        val odotetut =
            kentat.filter { it.pakollisuus != Pakollisuus.VOI_PUUTTUA || solmu.has(it.nimi) }.map { it.nimi }
        val todelliset = solmu.properties().map { it.key }
        if (todelliset != odotetut) {
            return listOf("${etuliite.ifEmpty { "sanoma" }}: kentät $todelliset, dokumentoitu $odotetut")
        }
        return kentat.flatMap { kentta ->
            val arvo = solmu.get(kentta.nimi)
            when {
                !kentta.kaytossa -> {
                    listOfNotNull(
                        "${kentta.polku}: merkitty ei käytössä, mutta arvo on $arvo".takeUnless { arvo.isNull },
                    )
                }

                arvo == null || arvo.isNull -> {
                    listOfNotNull(
                        "${kentta.polku}: tyhjä, vaikka dokumentoitu pakolliseksi".takeIf {
                            kentta.pakollisuus == Pakollisuus.AINA
                        },
                        "${kentta.polku}: esimerkkisanomassa on oltava arvo".takeIf {
                            kentta.muoto is Muoto.Tietoryhma
                        },
                    )
                }

                kentta.lista == Lista.EI -> {
                    tarkistaArvo(kentta, arvo)
                }

                !arvo.isArray || arvo.isEmpty -> {
                    listOf("${kentta.polku}: esimerkkisanomassa on oltava vähintään yksi alkio")
                }

                else -> {
                    arvo.flatMap { tarkistaArvo(kentta, it) }
                }
            }
        }
    }

    private fun tarkistaArvo(
        kentta: Kentta,
        arvo: JsonNode,
    ): List<String> {
        val muoto = kentta.muoto
        val kelpaa =
            when (muoto) {
                Muoto.Teksti -> {
                    arvo.isString
                }

                Muoto.Tunniste -> {
                    arvo.isString && Oid.parse(arvo.asString()).isRight()
                }

                Muoto.Paivamaara -> {
                    arvo.isString && runCatching { LocalDate.parse(arvo.asString()) }.isSuccess
                }

                Muoto.Aikaleima -> {
                    arvo.isString && runCatching { OffsetDateTime.parse(arvo.asString()) }.isSuccess
                }

                Muoto.KyllaEi -> {
                    arvo.isBoolean
                }

                Muoto.EiKaytossa -> {
                    arvo.isNull
                }

                is Muoto.Valinta -> {
                    arvo.isString && arvo.asString() in muoto.arvot.map { it.koodi }
                }

                is Muoto.Koodi -> {
                    arvo.properties().map { it.key } == listOf("koodiarvo", "koodistoUri") &&
                        arvo["koodistoUri"].asString() == muoto.koodistoUri &&
                        (muoto.arvot.isEmpty() || arvo["koodiarvo"].asString() in muoto.arvot.map { it.koodi })
                }

                is Muoto.Tietoryhma -> {
                    return if (arvo.isObject) {
                        tarkista(kentta.lapset, arvo, "${kentta.polku}.")
                    } else {
                        listOf("${kentta.polku}: odotettiin tietoryhmää, arvo on $arvo")
                    }
                }
            }
        return listOfNotNull("${kentta.polku}: arvo $arvo ei vastaa dokumentoitua muotoa $muoto".takeUnless { kelpaa })
    }
}
