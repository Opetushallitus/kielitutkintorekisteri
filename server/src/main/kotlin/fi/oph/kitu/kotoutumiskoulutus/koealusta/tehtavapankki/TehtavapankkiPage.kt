package fi.oph.kitu.kotoutumiskoulutus.koealusta.tehtavapankki

import fi.oph.kitu.html.Page
import fi.oph.kitu.html.card
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.finnishDate
import fi.oph.kitu.i18n.finnishDateTime
import fi.oph.kitu.i18n.unaryPlus
import fi.oph.kitu.tehtavapankki.TehtavapakettiEntity
import fi.oph.kitu.util.TimeService
import fi.oph.kitu.webmvc.Links
import kotlinx.html.a
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.p
import kotlinx.html.small
import kotlinx.html.table
import kotlinx.html.tbody
import kotlinx.html.td
import kotlinx.html.th
import kotlinx.html.thead
import kotlinx.html.tr
import kotlin.math.log10
import kotlin.math.pow

object TehtavapankkiPage {
    private fun formatBytes(bytes: Long): String {
        if (bytes < 1024) return "$bytes B"
        val units = arrayOf("B", "KB", "MB", "GB", "TB")
        val digitGroups = (log10(bytes.toDouble()) / log10(1024.0)).toInt()
        val value = bytes / 1024.0.pow(digitGroups.toDouble())
        return "%.2f %s".format(value, units[digitGroups])
    }

    private fun summaryLine(paketti: TehtavapakettiEntity): String? {
        val parts =
            buildList {
                paketti.lahdeLanguage?.let { add(languageLabel(it)) }
                paketti.lahdeVersion?.let { add("${UiText.Koto.versioLabel} $it") }
                paketti.lahdeFilegenerated?.let {
                    add(
                        "${UiText.Koto.generoituLabel} ${it.atZoneSameInstant(
                            TimeService.zoneId,
                        ).toLocalDate().finnishDate()}",
                    )
                }
            }
        return parts.takeIf { it.isNotEmpty() }?.joinToString(" · ")
    }

    fun render(
        tehtavapaketit: Map<String, List<TehtavapakettiObject>>,
        pakettiIdsByS3Avain: Map<String, Int> = emptyMap(),
        latestPakettiByGroup: Map<String, TehtavapakettiEntity?> = emptyMap(),
    ): String =
        Page.renderHtml(ohje = UiText.Ohje.Koto.tehtavapankki) {
            h1 { +UiText.Koto.tehtavapankki }

            if (tehtavapaketit.isEmpty()) {
                p { +UiText.Koto.eiTehtavapaketteja }
            } else {
                tehtavapaketit.forEach { (group, tps) ->
                    h2 { +group }
                    latestPakettiByGroup[group]?.let { paketti ->
                        summaryLine(paketti)?.let { line ->
                            p { small { +line } }
                        }
                    }
                    card(overflowAuto = true, compact = true) {
                        table(classes = "compact striped") {
                            thead {
                                tr {
                                    th { +UiText.Koto.siirretty }
                                    th { +UiText.Koto.koko }
                                    th { +UiText.Koto.sisalto }
                                }
                            }
                            tbody {
                                tps.forEachIndexed { index, tp ->
                                    tr(classes = if (index > 0) "faded" else null) {
                                        td { finnishDateTime(tp.timestamp) }
                                        td { +formatBytes(tp.size) }
                                        td {
                                            val pakettiId = pakettiIdsByS3Avain[tp.key]
                                            if (pakettiId != null) {
                                                // Paketti löytyy DB:stä — linkki näkymään, josta voi myös ladata XML:n.
                                                a(
                                                    href = Links.Tehtavapankki.paketti(pakettiId),
                                                ) { +UiText.Koto.naytaSisalto }
                                            } else {
                                                // Ei selattavaa versiota — tarjotaan raaka XML.
                                                a(href = Links.Tehtavapankki.download(tp.key)) {
                                                    attributes["download"] = ""
                                                    +UiText.Koto.lataaXml
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
}
