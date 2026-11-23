package fi.oph.kitu.tietosisalto

import tools.jackson.databind.json.JsonMapper

const val PAIVITYSKOMENTO = "./mvnw test -Dtest=LahtevatTiedotDokumenttiTest -Dkitu.dokumentaatio.paivita=true"

fun renderLahtevatTiedot(
    virrat: List<Tietovirta>,
    mapper: JsonMapper,
): String =
    buildString {
        appendLine("# Rekisteristä lähtevät tiedot")
        appendLine()
        appendLine("> Tämä sivu on tuotettu automaattisesti ohjelmakoodista, eikä sitä muokata käsin.")
        appendLine("> Sivu muuttuu aina, kun lähetettävät tiedot muuttuvat.")
        appendLine()
        appendLine(
            "Sivu kertoo, mitä tietoja Kielitutkintorekisteri lähettää muihin järjestelmiin. Jokaisesta " +
                "tietovirrasta kerrotaan vastaanottaja, lähetyksen ajankohta ja tarkoitus sekä kaikki " +
                "lähetettävät tiedot. Integraatioiden tekninen toteutus on kuvattu sivulla " +
                "[Integraatiot](./integraatiot.md).",
        )
        appendLine()
        virrat.forEach { appendLine("- [${it.otsikko}](#${it.ankkuri})") }
        appendLine()
        appendLine("## Taulukoiden lukuohje")
        appendLine()
        appendLine("- **Tieto** kertoo, mistä tiedosta on kyse.")
        appendLine("- **Pakollinen** kertoo, lähetetäänkö tieto aina (kyllä) vai voiko se jäädä tyhjäksi (ei).")
        appendLine("- **Muoto ja sallitut arvot** kertoo, millainen arvo on. Jos arvo valitaan ennalta sovituista")
        appendLine("  vaihtoehdoista, vaihtoehdot on lueteltu: ensin lähetettävä koodi ja suluissa sen merkitys.")
        appendLine("  Tietoryhmä koostuu useasta tiedosta, jotka on lueteltu omassa taulukossaan; linkki vie siihen.")
        appendLine("- **Tekninen nimi** on tiedon nimi lähetettävässä sanomassa. Merkintä `[]` tarkoittaa listaa.")

        virrat.forEach { virta ->
            appendLine()
            appendLine("<a id=\"${virta.ankkuri}\"></a>")
            appendLine()
            appendLine("## ${virta.otsikko}")
            appendLine()
            appendLine("- **Vastaanottaja:** ${virta.vastaanottaja}")
            appendLine("- **Milloin:** ${virta.milloin}")
            appendLine("- **Miksi:** ${virta.miksi}")
            virta.huomiot.forEach { appendLine("- **Huomio:** $it") }
            appendLine()
            appendTaulukko(virta, kentat(virta, mapper))
        }

        appendLine()
        appendLine("## Sivun päivittäminen")
        appendLine()
        appendLine(
            "Kehittäjälle: tietojen kuvaukset ovat koodissa `@Kuvaus`-annotaatioissa ja tietovirtojen " +
                "tiedot luokassa `Tietovirrat`. Sivu tuotetaan uudelleen hakemistossa `server/` komennolla",
        )
        appendLine()
        appendLine("```shell")
        appendLine(PAIVITYSKOMENTO)
        appendLine("```")
    }

private class Rivi(
    val kentta: Kentta,
    val arvo: Kentta,
    val nimi: String,
    val pakollinen: Boolean,
)

private fun nimi(kentta: Kentta): String = kentta.nimi + if (kentta.lista == Lista.EI) "" else "[]"

private fun rivi(kentta: Kentta): Rivi {
    val ainoa = kentta.lapset.filter { it.kaytossa }.singleOrNull()
    val pakollinen = kentta.pakollisuus == Pakollisuus.AINA
    return if (ainoa == null || kentta.lista != Lista.EI) {
        Rivi(kentta, kentta, nimi(kentta), pakollinen)
    } else {
        rivi(ainoa).let { Rivi(kentta, it.arvo, "${kentta.nimi}.${it.nimi}", pakollinen && it.pakollinen) }
    }
}

private fun ankkuri(
    virta: Tietovirta,
    kentta: Kentta,
): String =
    virta.ankkuri + "-" +
        kentta.polku
            .replace("[]", "")
            .lowercase()
            .replace('ä', 'a')
            .replace('ö', 'o')
            .replace('.', '-')

private fun StringBuilder.appendTaulukko(
    virta: Tietovirta,
    kentat: List<Kentta>,
) {
    val rivit = kentat.filter { it.kaytossa }.map(::rivi)
    val (ryhmat, arvot) = rivit.partition { it.arvo.muoto is Muoto.Tietoryhma }
    appendLine("| Tieto | Pakollinen | Muoto ja sallitut arvot | Tekninen nimi |")
    appendLine("| --- | --- | --- | --- |")
    rivit.forEach { rivi ->
        val tieto = listOfNotNull(rivi.kentta.kuvaus, rivi.kentta.lisatieto.takeIf { rivi in arvot }).joinToString(". ")
        val solut = listOf(tieto, if (rivi.pakollinen) "kyllä" else "ei", muoto(virta, rivi.arvo), "`${rivi.nimi}`")
        solut.forEach { check('|' !in it && '\n' !in it) { "Taulukon solussa on kielletty merkki: $it" } }
        appendLine(solut.joinToString(" | ", "| ", " |"))
    }
    ryhmat.forEach { rivi ->
        appendLine()
        appendLine("<a id=\"${ankkuri(virta, rivi.arvo)}\"></a>")
        appendLine()
        appendLine("### ${rivi.kentta.kuvaus}")
        appendLine()
        appendLine(
            listOfNotNull(rivi.kentta.lisatieto, "Tietoryhmän tekninen nimi sanomassa on `${rivi.arvo.polku}`.")
                .joinToString(" "),
        )
        appendLine()
        appendTaulukko(virta, rivi.arvo.lapset)
    }
}

private fun muoto(
    virta: Tietovirta,
    kentta: Kentta,
): String {
    val muoto = kentta.muoto
    val arvo =
        when (muoto) {
            Muoto.Teksti -> "teksti"
            Muoto.Tunniste -> "tunniste (OID)"
            Muoto.Paivamaara -> "päivämäärä (vvvv-kk-pp)"
            Muoto.Aikaleima -> "päivämäärä ja kellonaika"
            Muoto.KyllaEi -> "kyllä tai ei"
            Muoto.EiKaytossa -> error("Käyttämätöntä kenttää ${kentta.polku} ei tulosteta")
            is Muoto.Valinta -> muoto.arvot.joinToString(", ") { arvo(it) }
            is Muoto.Koodi -> koodi(muoto)
            is Muoto.Tietoryhma -> "[tietoryhmä](#${ankkuri(virta, kentta)})"
        }
    return when (kentta.lista) {
        Lista.EI -> arvo
        Lista.LISTA -> "lista: $arvo"
        Lista.VAHINTAAN_YKSI -> "lista, jossa on vähintään yksi: $arvo"
    }
}

private fun koodi(muoto: Muoto.Koodi): String =
    when {
        muoto.arvot.isEmpty() -> "koodi koodistosta `${muoto.koodistoUri}` (${muoto.selite})"
        muoto.arvot.size == 1 -> "aina sama arvo: ${arvo(muoto.arvot.single())}"
        else -> muoto.arvot.joinToString(", ") { arvo(it) }
    }

private fun arvo(arvo: Arvo): String =
    if (arvo.nimi == null || arvo.nimi.equals(arvo.koodi, ignoreCase = true)) {
        "`${arvo.koodi}`"
    } else {
        "`${arvo.koodi}` (${arvo.nimi})"
    }
