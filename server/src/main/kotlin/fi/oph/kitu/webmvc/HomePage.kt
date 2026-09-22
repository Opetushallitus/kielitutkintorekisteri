package fi.oph.kitu.webmvc

import fi.oph.kitu.html.Page
import fi.oph.kitu.html.ViewMessageData
import fi.oph.kitu.html.card
import fi.oph.kitu.html.cardContent
import fi.oph.kitu.html.classes
import fi.oph.kitu.html.formPost
import fi.oph.kitu.html.javascript
import fi.oph.kitu.html.testId
import fi.oph.kitu.html.viewMessage
import fi.oph.kitu.html.warningMessage
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.i18n.formatRelativeTime
import fi.oph.kitu.i18n.tolgee.TolgeeSyncResult
import fi.oph.kitu.i18n.tolgee.TolgeeSyncStatus
import fi.oph.kitu.i18n.unaryPlus
import kotlinx.html.FlowContent
import kotlinx.html.UL
import kotlinx.html.a
import kotlinx.html.button
import kotlinx.html.div
import kotlinx.html.h1
import kotlinx.html.h2
import kotlinx.html.li
import kotlinx.html.span
import kotlinx.html.stream.createHTML
import kotlinx.html.ul
import java.time.Instant

object HomePage {
    private const val SKELETON_ROW_COUNT = 5

    fun render(message: ViewMessageData? = null): String =
        Page.renderHtml {
            h1 { +UiText.appTitle }
            viewMessage(message)
            tolgeeSyncWarning()
            div(classes = "grid dashboard-grid") {
                testId("dashboard")
                lazyCard(groupId = "yki", contentKey = "yki", title = UiText.Nav.yki)
                lazyCard(groupId = "vkt", contentKey = "vkt", title = UiText.Nav.vkt)
                lazyCard(
                    groupId = "koto-kielitesti",
                    contentKey = "koto",
                    title = UiText.Nav.kotoutumiskoulutuksenPaattotesti,
                )
                lazyCard(groupId = "admin", contentKey = "admin", title = UiText.Nav.yllapito)
            }
            javascript(loaderScript())
        }

    fun renderYkiCardContent(s: YkiStats): String =
        createHTML().ul(classes = "dashboard-stats") {
            ykiRows(s)
        }

    fun renderVktCardContent(s: VktStats): String =
        createHTML().ul(classes = "dashboard-stats") {
            vktRows(s)
        }

    fun renderKotoCardContent(s: KotoStats): String =
        createHTML().ul(classes = "dashboard-stats") {
            kotoRows(s)
        }

    fun renderAdminCardContent(s: AdminStats): String =
        createHTML().ul(classes = "dashboard-stats") {
            adminRows(s)
        }

    private fun UL.ykiRows(s: YkiStats) {
        statRow(UiText.Nav.suoritukset, s.suoritusCount, Links.Yki.suoritukset())
        statRow(UiText.Nav.arvioijat, s.arvioijaCount, Links.Yki.arvioijat())
        statRow(
            UiText.Nav.tarkistusarvioinnit,
            s.tarkistusarvioinnitOdottamassaCount,
            Links.Yki.tarkistusArvioinnit(),
        )
        statRow(
            label = UiText.Etusivu.arvioijienSolkiVirheet,
            value = s.arvioijaSolkiErrorCount,
            href = Links.Yki.arvioijat() + "?vainSolkiVirheet=true",
            errorIfNonZero = true,
        )
        statRow(
            label = UiText.Yki.suoritustenTuonninVirheet,
            value = s.suoritusImportErrorCount,
            href = Links.Yki.suorituksetVirheet(),
            errorIfNonZero = true,
        )
        statRow(
            label = UiText.Etusivu.koskiSiirronVirheet,
            value = s.koskiErrorCount,
            href = Links.Yki.koskiVirheet(),
            errorIfNonZero = true,
        )
        latestReceivedRow(s.latestReceivedAt, Links.Yki.suoritukset())
    }

    private fun UL.vktRows(s: VktStats) {
        statRow(UiText.Nav.kaikkiSuoritukset, s.suoritusCount, Links.Vkt.suoritukset())
        statRow(
            label = UiText.Nav.erinomaisenTaidonIlmoittautuneet,
            value = s.ilmoittautuneetErinomaisenTaso,
            href = Links.Vkt.erinomaisenTaitotasonIlmoittautuneet(),
        )
        statRow(
            label = UiText.Nav.erinomaisenTaidonSuoritukset,
            value = s.suorituksetErinomaisenTaso,
            href = Links.Vkt.erinomaisenTaitotasonArvioidutSuoritukset(),
        )
        statRow(
            label = UiText.Nav.hyvanJaTyydyttavanSuoritukset,
            value = s.suorituksetHyvaJaTyydyttavaTaso,
            href = Links.Vkt.hyvanJaTyydyttavanTaitotasonSuoritukset(),
        )
        statRow(
            label = UiText.Etusivu.koskiSiirronVirheet,
            value = s.koskiErrorCount,
            href = Links.Vkt.koskiVirheet(),
            errorIfNonZero = true,
        )
        latestReceivedRow(s.latestReceivedAt, Links.Vkt.suoritukset())
    }

    private fun UL.kotoRows(s: KotoStats) {
        statRow(UiText.Nav.suoritukset, s.suoritusCount, Links.Kielitesti.suoritukset())
        statRow(UiText.Nav.tehtavapaketit, s.tehtavapaketitCount, Links.Tehtavapankki.list())
        statRow(
            label = UiText.Etusivu.tuonninVirheet,
            value = s.importErrorCount,
            href = Links.Kielitesti.virheet(),
            errorIfNonZero = true,
        )
        latestReceivedRow(s.latestReceivedAt, Links.Kielitesti.suoritukset())
    }

    private fun UL.adminRows(s: AdminStats) {
        statRow(UiText.Etusivu.kaynnissaOlevatErajot, s.runningCount, Links.Admin.dbScheduler())
        statRow(
            label = UiText.Etusivu.erajotVirhetilassa,
            value = s.failingCount,
            href = Links.Admin.dbScheduler(),
            errorIfNonZero = true,
        )
        statRow(UiText.Nav.erajojenHallinta, value = null, href = Links.Admin.dbScheduler())
        actionRow(
            label = UiText.Etusivu.tyhjennaKaannosvalimuisti,
            action = Links.Admin.tyhjennaKaannosvalimuisti(),
            rowTestId = "tyhjenna-kaannosvalimuisti",
        )
    }

    private fun UL.actionRow(
        label: LocalizedString,
        action: String,
        rowTestId: String,
    ) = li(classes = "stat-row") {
        formPost(action) {
            button(classes = "stat-link") {
                testId(rowTestId)
                span(classes = "stat-label") { +label }
                span(classes = "stat-value") { +"→" }
            }
        }
    }

    private fun FlowContent.lazyCard(
        groupId: String,
        contentKey: String,
        title: LocalizedString,
    ) = card {
        testId("$groupId-links")
        cardContent {
            h2(classes = "dashboard-card-title") { +title }
            ul(classes = "dashboard-stats skeleton-stats") {
                attributes["data-card-content"] = contentKey
                attributes["aria-busy"] = "true"
                repeat(SKELETON_ROW_COUNT) {
                    li(classes = "stat-row skeleton-row") {
                        span(classes = "skeleton skeleton-label")
                        span(classes = "skeleton skeleton-value")
                    }
                }
            }
        }
    }

    private fun UL.statRow(
        label: LocalizedString,
        value: Long?,
        href: String,
        errorIfNonZero: Boolean = false,
        warnIfNonZero: Boolean = false,
    ) {
        val nonZero = value != null && value > 0L
        val badgeClass =
            when {
                !nonZero -> null
                errorIfNonZero -> "badge badge-error"
                warnIfNonZero -> "badge badge-warning"
                else -> null
            }
        li(classes = "stat-row") {
            a(href = href, classes = "stat-link") {
                span(classes = "stat-label") { +label }
                span(classes = classes(true to "stat-value", (badgeClass != null) to (badgeClass ?: ""))) {
                    +(value?.toString() ?: "→")
                }
            }
        }
    }

    private fun UL.latestReceivedRow(
        latestReceivedAt: Instant?,
        href: String,
    ) = li(classes = "stat-row") {
        a(href = href, classes = "stat-link") {
            span(classes = "stat-label") { +UiText.Etusivu.viimeisinSaapunutSuoritus }
            span(classes = "stat-value stat-value-muted") {
                testId("latest-received")
                +formatRelativeTime(latestReceivedAt)
            }
        }
    }

    private fun loaderScript(): String {
        val ykiUrl = Links.Dashboard.yki()
        val vktUrl = Links.Dashboard.vkt()
        val kotoUrl = Links.Dashboard.koto()
        val adminUrl = Links.Dashboard.admin()
        return """
            const cards = [
                { id: "yki",   url: "$ykiUrl" },
                { id: "vkt",   url: "$vktUrl" },
                { id: "koto",  url: "$kotoUrl" },
                { id: "admin", url: "$adminUrl" },
            ];
            cards.forEach(({ id, url }) => {
                const target = document.querySelector('[data-card-content="' + id + '"]');
                if (!target) return;
                fetch(url, { headers: { Accept: "text/html" } })
                    .then(r => r.ok ? r.text() : Promise.reject(r.status))
                    .then(html => { target.outerHTML = html; })
                    .catch(() => {
                        target.removeAttribute("aria-busy");
                        target.innerHTML = '<li class="stat-row">Tietojen lataus epäonnistui.</li>';
                    });
            });
            """.trimIndent()
    }
}

internal fun FlowContent.tolgeeSyncWarning() {
    val viesti =
        when (val tila = TolgeeSyncStatus.last) {
            is TolgeeSyncResult.PoistorajaYlittyi -> {
                "Tolgee-synkronointi ohitti poistot: poistettavia avaimia ${tila.poistettavia}, " +
                    "turvaraja ${tila.raja}. Tarkista muutokset ennen kuin nostat rajaa."
            }

            TolgeeSyncResult.RekisteriTyhja -> {
                "Tolgee-synkronointi ohitettiin: koodin käännösavainrekisteri oli tyhjä."
            }

            is TolgeeSyncResult.Virhe -> {
                "Tolgee-synkronointi epäonnistui: ${tila.syy}"
            }

            is TolgeeSyncResult.Ok, null -> {
                return
            }
        }
    warningMessage(LocalizedString(fi = viesti))
}
