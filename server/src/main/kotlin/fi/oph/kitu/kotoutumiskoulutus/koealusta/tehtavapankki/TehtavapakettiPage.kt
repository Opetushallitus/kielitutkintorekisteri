package fi.oph.kitu.kotoutumiskoulutus.koealusta.tehtavapankki

import fi.oph.kitu.html.Page
import fi.oph.kitu.html.card
import fi.oph.kitu.html.cardContent
import fi.oph.kitu.html.infoTable
import fi.oph.kitu.html.safeHtml
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDateTime
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.tehtavapankki.TehtavaEntity
import fi.oph.kitu.tehtavapankki.TehtavaTiedostoEntity
import fi.oph.kitu.tehtavapankki.TehtavaVastausEntity
import fi.oph.kitu.tehtavapankki.TehtavapakettiEntity
import fi.oph.kitu.tehtavapankki.TehtavaryhmaEntity
import fi.oph.kitu.webmvc.Links
import kotlinx.html.FlowContent
import kotlinx.html.SECTION
import kotlinx.html.a
import kotlinx.html.article
import kotlinx.html.audio
import kotlinx.html.b
import kotlinx.html.code
import kotlinx.html.details
import kotlinx.html.div
import kotlinx.html.figcaption
import kotlinx.html.figure
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.h3
import kotlinx.html.img
import kotlinx.html.li
import kotlinx.html.p
import kotlinx.html.pre
import kotlinx.html.section
import kotlinx.html.small
import kotlinx.html.span
import kotlinx.html.strong
import kotlinx.html.summary
import kotlinx.html.title
import kotlinx.html.ul
import kotlinx.html.unsafe
import tools.jackson.databind.JsonNode
import tools.jackson.databind.node.ArrayNode
import tools.jackson.databind.node.NumericNode
import tools.jackson.databind.node.ObjectNode
import tools.jackson.databind.node.StringNode
import java.net.URLDecoder
import java.nio.charset.StandardCharsets

object TehtavapakettiPage {
    fun render(
        paketti: TehtavapakettiEntity,
        ryhmat: List<TehtavaryhmaEntity>,
        tehtavatByRyhma: Map<Int, List<TehtavaEntity>>,
        vastauksetByTehtava: Map<Int, List<TehtavaVastausEntity>>,
        tiedostotByTehtava: Map<Int, List<TehtavaTiedostoEntity>>,
    ): String =
        Page.renderHtml(ohje = UiText.Ohje.Koto.tehtavapaketti) {
            renderHeader(paketti)
            if (ryhmat.isEmpty()) {
                p { +UiText.Koto.paketissaEiRyhmia }
            } else {
                ryhmat.forEach { ryhma ->
                    renderRyhma(
                        ryhma = ryhma,
                        tehtavat = tehtavatByRyhma[ryhma.id] ?: emptyList(),
                        vastauksetByTehtava = vastauksetByTehtava,
                        tiedostotByTehtava = tiedostotByTehtava,
                        assetPrefix = paketti.assetPrefix(),
                    )
                }
            }
        }

    private fun SECTION.renderHeader(paketti: TehtavapakettiEntity) {
        h1 { +paketti.nimi.stripMoodlePrefix() }
        card(compact = true) {
            infoTable(
                UiText.Koto.lahdejarjestelma to { +paketti.lahdejarjestelma },
                UiText.Koto.lahdeId to { +paketti.lahdeId },
                UiText.Koto.versio to {
                    code { +paketti.versioHash.take(12) }
                },
                paketti.lahdeVersion?.let { v -> UiText.Koto.lahdeversio to { code { +v } } },
                paketti.lahdeLanguage?.let { lang -> UiText.Koto.kieli to { +languageLabel(lang) } },
                paketti.lahdePublished?.let { pub ->
                    UiText.Koto.kurssinAlku to { finnishDateTime(pub.toInstant()) }
                },
                paketti.lahdeFilegenerated?.let { gen ->
                    UiText.Koto.lahdeGeneroitu to { finnishDateTime(gen.toInstant()) }
                },
                UiText.Koto.ladattu to {
                    paketti.luotu?.let { finnishDateTime(it.toInstant()) } ?: +"–"
                },
                paketti.s3Avain?.let { key ->
                    UiText.Koto.xmlTiedosto to {
                        a(href = downloadUrl(key)) {
                            attributes["download"] = ""
                            +UiText.Koto.lataa
                        }
                    }
                },
            )
        }
    }

    private fun String.stripMoodlePrefix() =
        this.replaceFirst(Regex("^\\$\\w+\\$/\\w+/?"), "").takeIf { it.isNotBlank() }
            ?: UiText.Koto.tyhjaNimi.toString()

    private fun SECTION.renderRyhma(
        ryhma: TehtavaryhmaEntity,
        tehtavat: List<TehtavaEntity>,
        vastauksetByTehtava: Map<Int, List<TehtavaVastausEntity>>,
        tiedostotByTehtava: Map<Int, List<TehtavaTiedostoEntity>>,
        assetPrefix: String?,
    ) {
        section {
            h2 { +"${ryhma.jarjestys}. ${ryhma.nimi.stripMoodlePrefix()}" }
            card {
                if (tehtavat.isEmpty()) {
                    p { +UiText.Koto.eiTehtavia }
                } else {
                    tehtavat.forEach { tehtava ->
                        renderTehtava(
                            tehtava = tehtava,
                            vastaukset = vastauksetByTehtava[tehtava.id] ?: emptyList(),
                            tiedostot = tiedostotByTehtava[tehtava.id] ?: emptyList(),
                            assetPrefix = assetPrefix,
                        )
                    }
                }
            }
        }
    }

    private fun FlowContent.renderTehtava(
        tehtava: TehtavaEntity,
        vastaukset: List<TehtavaVastausEntity>,
        tiedostot: List<TehtavaTiedostoEntity>,
        assetPrefix: String?,
    ) {
        article {
            h3("tehtava-otsikko") {
                strong { +"${tehtava.jarjestys}. ${tehtava.nimi ?: UiText.Koto.nimeton.toString()}" }
                small { +moodleTehtavatyyppiNimi(tehtava.tyyppi) }
            }
            tehtava.lahdeId?.let {
                p {
                    b {
                        +UiText.Koto.tehtavanTunniste
                        +":"
                    }
                    +" $it"
                }
            }
            if (!tehtava.teksti.isNullOrBlank()) {
                renderRichText(tehtava.teksti, tehtava.tekstinFormaatti, assetPrefix)
            }
            if (vastaukset.isNotEmpty()) {
                p("vaihtoehdot-otsikko") { strong { +UiText.Koto.vastausvaihtoehdot } }
                ul("vaihtoehdot") {
                    vastaukset.forEach { vastaus ->
                        li {
                            val fraction = vastaus.metadata.get("fraction")?.asString()
                            if (fraction != null) {
                                code("fraction") { +fraction }
                            }
                            renderRichTextInline(vastaus.teksti, vastaus.tekstinFormaatti, assetPrefix)
                        }
                    }
                }
            }
            // Älä toista @@PLUGINFILE@@-viittauksien kautta tekstiin upotettuja
            // mediatiedostoja erikseen liitelistana.
            val inlineAssetNames = collectInlineAssetNames(tehtava, vastaukset)
            val visibleTiedostot = tiedostot.filterNot { it.tiedostonimi in inlineAssetNames }
            if (visibleTiedostot.isNotEmpty()) {
                renderTiedostot(visibleTiedostot)
            }

            tehtavaMetadata(tehtava.metadata)
        }
    }

    private fun collectInlineAssetNames(
        tehtava: TehtavaEntity,
        vastaukset: List<TehtavaVastausEntity>,
    ): Set<String> {
        val names = mutableSetOf<String>()
        if (tehtava.tekstinFormaatti == "html" && !tehtava.teksti.isNullOrBlank()) {
            names += extractPluginFileNames(tehtava.teksti)
        }
        vastaukset.forEach { v ->
            if (v.tekstinFormaatti == "html" && !v.teksti.isNullOrBlank()) {
                names += extractPluginFileNames(v.teksti)
            }
        }
        return names
    }

    private fun FlowContent.renderTiedostot(tiedostot: List<TehtavaTiedostoEntity>) {
        val grouped = tiedostot.groupBy { it.mediaKind() }
        grouped[MediaKind.IMAGE]?.forEach { tiedosto ->
            figure {
                img(src = downloadUrl(tiedosto.s3Avain), alt = tiedosto.tiedostonimi)
                figcaption {
                    a(href = downloadUrl(tiedosto.s3Avain)) {
                        attributes["download"] = ""
                        +tiedosto.tiedostonimi
                    }
                }
            }
        }
        grouped[MediaKind.AUDIO]?.forEach { tiedosto ->
            figure {
                audio {
                    controls = true
                    src = downloadUrl(tiedosto.s3Avain)
                }
                figcaption {
                    a(href = downloadUrl(tiedosto.s3Avain)) {
                        attributes["download"] = ""
                        +tiedosto.tiedostonimi
                    }
                }
            }
        }
        val muut = grouped[MediaKind.OTHER].orEmpty()
        if (muut.isNotEmpty()) {
            strong {
                +UiText.Koto.liitetiedostot
                +":"
            }
            ul {
                muut.forEach { tiedosto ->
                    li {
                        a(href = downloadUrl(tiedosto.s3Avain)) {
                            attributes["download"] = ""
                            +tiedosto.tiedostonimi
                        }
                    }
                }
            }
        }
    }

    private fun FlowContent.renderRichText(
        text: String,
        format: String?,
        assetPrefix: String?,
    ) {
        val cleaned = stripPoodllPlaceholders(text)
        if (format == "html") {
            // Lähde on Moodlen virkailijakohtainen export, ei loppukäyttäjäsyöte.
            // S3-asset-viittaukset (@@PLUGINFILE@@/...) kirjoitetaan oikeiksi
            // download-linkeiksi, jotta upotetut <audio>/<img>-elementit toimivat.
            div("tehtava-teksti") {
                unsafe { +rewriteMoodleAssetUrls(cleaned, assetPrefix) }
            }
        } else {
            pre("tehtava-teksti") { +cleaned }
        }
    }

    private fun FlowContent.renderRichTextInline(
        text: String?,
        format: String?,
        assetPrefix: String?,
    ) {
        if (text.isNullOrBlank()) return
        val cleaned = stripPoodllPlaceholders(text)
        if (cleaned.isBlank()) return
        if (format == "html") {
            span {
                unsafe { +rewriteMoodleAssetUrls(cleaned, assetPrefix) }
            }
        } else {
            +cleaned
        }
    }
}

private enum class MediaKind { IMAGE, AUDIO, OTHER }

private val imageExtensions = setOf("jpg", "jpeg", "png", "gif", "webp", "svg", "bmp", "avif")
private val audioExtensions = setOf("mp3", "wav", "ogg", "oga", "m4a", "aac", "flac", "weba")

private fun TehtavaTiedostoEntity.mediaKind(): MediaKind {
    val ext = tiedostonimi.substringAfterLast('.', "").lowercase()
    return when (ext) {
        in imageExtensions -> MediaKind.IMAGE
        in audioExtensions -> MediaKind.AUDIO
        else -> MediaKind.OTHER
    }
}

private fun TehtavapakettiEntity.assetPrefix(): String? = s3Avain?.let { "${it.removeSuffix(".xml")} assets/" }

private fun downloadUrl(s3Avain: String): String = Links.Tehtavapankki.download(s3Avain)

private val pluginFileRegex = Regex("@@PLUGINFILE@@(?:/|%2F)([^\"'<>\\s)]+)")

// Moodlen Poodll-pluginin sisäänkirjoittama placeholder, esim.
// `{POODLL:type="pw-multiplayeraudio",canplaycount="1",...}`. Korvataan
// renderoitavasta tekstistä, koska virkailijalle siitä ei ole hyötyä eikä
// sisältö ole varsinaista kysymystekstiä.
private val poodllPlaceholderRegex = Regex("\\{POODLL:[^}]*}")

internal fun stripPoodllPlaceholders(text: String): String = poodllPlaceholderRegex.replace(text, "")

/**
 * Korvaa Moodlen `@@PLUGINFILE@@/<filename>`-viittaukset varsinaisilla
 * S3-download-linkeillä jotka osoittavat tämän paketin assets-kansioon.
 * Hyväksyy myös URL-koodatun version (`@@PLUGINFILE@@%2F...`). Tiedostonimi
 * URL-puretaan vertailua varten ja koodataan uudelleen avaimeksi, jotta
 * polut joissa on välejä tai ääkkösiä toimivat.
 */
internal fun rewriteMoodleAssetUrls(
    html: String,
    assetPrefix: String?,
): String {
    if (assetPrefix == null) return html
    return pluginFileRegex.replace(html) { match ->
        downloadUrl("$assetPrefix${decodePluginFileName(match.groupValues[1])}")
    }
}

private fun extractPluginFileNames(html: String): Set<String> =
    pluginFileRegex
        .findAll(html)
        .map { decodePluginFileName(it.groupValues[1]) }
        .toSet()

private fun decodePluginFileName(raw: String): String =
    try {
        URLDecoder.decode(raw, StandardCharsets.UTF_8)
    } catch (_: IllegalArgumentException) {
        raw
    }

// Moodlen tehtävätyyppien suomennokset. Kattaa Moodle 4.x:n vakio-tyypit ja
// yleiset kontribuutio-tyypit (mm. Koealustan käyttämä cloudpoodll).
// Tuntemattomalle tyypille palautetaan tyypin tekninen tunniste sellaisenaan.
internal fun moodleTehtavatyyppiNimi(tyyppi: String): String =
    when (tyyppi.lowercase()) {
        // Vakio-tyypit
        "multichoice" -> UiText.Koto.Tehtavatyyppi.monivalinta

        "truefalse" -> UiText.Koto.Tehtavatyyppi.tosiEpatosi

        "shortanswer" -> UiText.Koto.Tehtavatyyppi.lyhytVastaus

        "numerical" -> UiText.Koto.Tehtavatyyppi.numeerinenVastaus

        "essay" -> UiText.Koto.Tehtavatyyppi.essee

        "match" -> UiText.Koto.Tehtavatyyppi.yhdistaminen

        "multianswer" -> UiText.Koto.Tehtavatyyppi.cloze

        "calculated" -> UiText.Koto.Tehtavatyyppi.lasku

        "calculatedmulti" -> UiText.Koto.Tehtavatyyppi.monivalintaLasku

        "calculatedsimple" -> UiText.Koto.Tehtavatyyppi.yksinkertainenLasku

        "description" -> UiText.Koto.Tehtavatyyppi.ohjeteksti

        "ddwtos" -> UiText.Koto.Tehtavatyyppi.vetaPudotaTeksti

        "ddmarker" -> UiText.Koto.Tehtavatyyppi.vetaPudotaMerkit

        "ddimageortext" -> UiText.Koto.Tehtavatyyppi.vetaPudotaKuva

        "gapselect" -> UiText.Koto.Tehtavatyyppi.valitsePuuttuvat

        "random" -> UiText.Koto.Tehtavatyyppi.satunnais

        "randomsamatch" -> UiText.Koto.Tehtavatyyppi.satunnainenLyhytYhdistaminen

        "missingtype" -> UiText.Koto.Tehtavatyyppi.puuttuvaTyyppi

        // Yleiset kontribuutio-tyypit
        "cloudpoodll" -> UiText.Koto.Tehtavatyyppi.aaninauhoitus

        "recordrtc" -> UiText.Koto.Tehtavatyyppi.aaniVideonauhoitus

        "pmatch" -> UiText.Koto.Tehtavatyyppi.hahmonsovitus

        "pmatchjme" -> UiText.Koto.Tehtavatyyppi.kemiallinenKaava

        "coderunner" -> UiText.Koto.Tehtavatyyppi.ohjelmointi

        "stack" -> UiText.Koto.Tehtavatyyppi.stack

        "ordering" -> UiText.Koto.Tehtavatyyppi.jarjestaminen

        "combined" -> UiText.Koto.Tehtavatyyppi.yhdistelma

        "formulas" -> UiText.Koto.Tehtavatyyppi.kaava

        "gapfill" -> UiText.Koto.Tehtavatyyppi.aukko

        "regexp" -> UiText.Koto.Tehtavatyyppi.saannollinenLauseke

        "speakautograde" -> UiText.Koto.Tehtavatyyppi.puhetehtava

        "crossword" -> UiText.Koto.Tehtavatyyppi.ristikko

        "drawing" -> UiText.Koto.Tehtavatyyppi.piirto

        else -> null
    }?.toString() ?: tyyppi

// Koealustan kielikoodit ovat tyypillisesti ISO 639-2 kolmikirjaimisia
// (FIN/SWE/ENG). Esitetään virkailijalle suomeksi; tuntematon koodi
// näytetään raakana, jotta uudet kielet eivät jää kokonaan piiloon.
internal fun languageLabel(code: String): String =
    when (code.lowercase()) {
        "fin" -> UiText.Koto.Kieli.fin
        "swe" -> UiText.Koto.Kieli.swe
        "eng" -> UiText.Koto.Kieli.eng
        "rus" -> UiText.Koto.Kieli.rus
        "est" -> UiText.Koto.Kieli.est
        "ara" -> UiText.Koto.Kieli.ara
        "fas" -> UiText.Koto.Kieli.fas
        "som" -> UiText.Koto.Kieli.som
        "ukr" -> UiText.Koto.Kieli.ukr
        else -> null
    }?.toString() ?: code

private fun FlowContent.tehtavaMetadata(data: JsonNode) {
    if (!data.isEmpty) {
        card {
            cardContent {
                details {
                    summary { +UiText.Koto.metadata }
                    tehtavaMetadataJson(data)
                }
            }
        }
    }
}

private fun FlowContent.tehtavaMetadataJson(node: JsonNode) {
    when (node) {
        is ArrayNode -> {
            ul { node.forEach { li { tehtavaMetadataJson(it) } } }
        }

        is ObjectNode -> {
            tehtavaMetadataObject(node)
        }

        is StringNode -> {
            +node.stringValue()
        }

        is NumericNode -> {
            +node.asString()
        }

        else -> {
            +node.toString()
        }
    }
}

private fun FlowContent.tehtavaMetadataObject(obj: ObjectNode) {
    val props = obj.properties()
    if (obj.has("text")) {
        when (obj.get("format").asString()) {
            "html" -> span { safeHtml(obj.get("text").asString()) }
            else -> tehtavaMetadataJson(obj.get("text"))
        }
    } else {
        ul {
            props.forEach { prop ->
                val (key, value) = tehtavaMetadataProperty(prop.key, prop.value)
                li {
                    title = value.toString()
                    b {
                        +key
                        +": "
                    }
                    span { tehtavaMetadataJson(value) }
                }
            }
        }
    }
}

private fun tehtavaMetadataProperty(
    key: String,
    value: JsonNode,
): Pair<String, JsonNode> =
    when (key) {
        "hidden" -> {
            UiText.Koto.Metatieto.piilotettu
                .toString() to translateBoolean(value)
        }

        "single" -> {
            UiText.Koto.Metatieto.vainYksiVastaus
                .toString() to translateBoolean(value)
        }

        "penalty" -> {
            UiText.Koto.Metatieto.rangaistuskerroin
                .toString() to value
        }

        "defaultgrade" -> {
            UiText.Koto.Metatieto.oletuspistemaara
                .toString() to value
        }

        "shuffleanswers" -> {
            UiText.Koto.Metatieto.sekoitaVastaukset
                .toString() to translateBoolean(value)
        }

        "answernumbering" -> {
            UiText.Koto.Metatieto.vastauksenNumeroiminen
                .toString() to value
        }

        "correctfeedback" -> {
            UiText.Koto.Metatieto.palauteOikeasta
                .toString() to value
        }

        "generalfeedback" -> {
            UiText.Koto.Metatieto.yleispalaute
                .toString() to value
        }

        "incorrectfeedback" -> {
            UiText.Koto.Metatieto.palauteVaarasta
                .toString() to value
        }

        "showstandardinstruction" -> {
            UiText.Koto.Metatieto.naytaVakioOhje
                .toString() to value
        }

        "partiallycorrectfeedback" -> {
            UiText.Koto.Metatieto.palauteOsittain
                .toString() to value
        }

        "responseformat" -> {
            UiText.Koto.Metatieto.vastausmuoto
                .toString() to value
        }

        "responsefieldlines" -> {
            UiText.Koto.Metatieto.vastauskentanRivimaara
                .toString() to value
        }

        "responserequired" -> {
            UiText.Koto.Metatieto.vastausPakollinen
                .toString() to translateBoolean(value)
        }

        "responsetemplate" -> {
            UiText.Koto.Metatieto.vastauspohja
                .toString() to value
        }

        "maxwordlimit" -> {
            UiText.Koto.Metatieto.sanamaaranEnimmais
                .toString() to value
        }

        "minwordlimit" -> {
            UiText.Koto.Metatieto.sanamaaranVahimmais
                .toString() to value
        }

        "attachments" -> {
            UiText.Koto.Metatieto.liitteidenMaara
                .toString() to value
        }

        "attachmentsrequired" -> {
            UiText.Koto.Metatieto.vaadittavatLiitteet
                .toString() to value
        }

        "maxbytes" -> {
            UiText.Koto.Metatieto.tiedostonEnimmaiskoko
                .toString() to value
        }

        "noaudiofilters" -> {
            UiText.Koto.Metatieto.eiAanenSuodattimia
                .toString() to translateBoolean(value)
        }

        "transcriber" -> {
            UiText.Koto.Metatieto.litteroija
                .toString() to translateBoolean(value)
        }

        "transcode" -> {
            UiText.Koto.Metatieto.koodausMuunnos
                .toString() to translateBoolean(value)
        }

        "audioskin" -> {
            UiText.Koto.Metatieto.aanisoittimenTeema
                .toString() to value
        }

        "videoskin" -> {
            UiText.Koto.Metatieto.videosoittimenTeema
                .toString() to value
        }

        "studentplayer" -> {
            UiText.Koto.Metatieto.opiskelijanSoitin
                .toString() to translateBoolean(value)
        }

        "teacherplayer" -> {
            UiText.Koto.Metatieto.opettajanSoitin
                .toString() to translateBoolean(value)
        }

        "timelimit" -> {
            UiText.Koto.Metatieto.aikaraja
                .toString() to value
        }

        "expiredays" -> {
            UiText.Koto.Metatieto.vanhentumispaivat
                .toString() to value
        }

        "language" -> {
            UiText.Koto.kieli.toString() to value
        }

        "tags" -> {
            UiText.Koto.Metatieto.tunnisteet
                .toString() to value
        }

        "safesave" -> {
            UiText.Koto.Metatieto.turvallinenTallennus
                .toString() to translateBoolean(value)
        }

        "usecase" -> {
            UiText.Koto.Metatieto.kayttotarkoitus
                .toString() to value
        }

        "graderinfo" -> {
            UiText.Koto.Metatieto.arviointiohjeet
                .toString() to value
        }

        else -> {
            key to value
        }
    }

private fun translateBoolean(node: JsonNode) =
    when (node.toString()) {
        "0", "false" -> StringNode(UiText.Filter.ei.toString())
        "1", "true" -> StringNode(UiText.Filter.kylla.toString())
        else -> node
    }
