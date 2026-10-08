package fi.oph.kitu.i18n

import fi.oph.kitu.html.testId
import fi.oph.kitu.util.TimeService
import kotlinx.html.FlowContent
import kotlinx.html.span
import kotlinx.html.title
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.format.DateTimeFormatter

private val finnishDateFormatter = DateTimeFormatter.ofPattern("d.M.yyyy")
private val isoDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
private val finnishDateTimeWithZoneFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ssX")
private val finnishDateTimeFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss")

fun LocalDate.finnishDate(): String = format(finnishDateFormatter)

fun LocalDate.isoDate(): String = format(isoDateFormatter)

fun aikarajausDescription(
    alku: LocalDate?,
    loppu: LocalDate?,
    lang: Language = CurrentLanguage.get(),
    prefix: LocalizedString = UiText.Filter.aikarajausPrefix,
): String? =
    if (alku != null || loppu != null) {
        listOf(alku?.finnishDate().orEmpty(), loppu?.finnishDate().orEmpty())
            .joinToString("-", prefix = prefix.get(lang) + ": ")
    } else {
        null
    }

fun Instant.finnishDateTime(includeTimeZone: Boolean = true): String =
    (if (includeTimeZone) finnishDateTimeWithZoneFormatter else finnishDateTimeFormatter)
        .format(this.atZone(TimeService.zoneId))

fun FlowContent.finnishDate(d: LocalDate) {
    span {
        testId("date")
        attributes["aria-current"] = "date"
        +d.finnishDate()
    }
}

fun FlowContent.finnishDateTime(dt: Instant) {
    span {
        testId("datetime")
        title = dt.toString()
        +dt.finnishDateTime(includeTimeZone = false)
    }
}

fun formatRelativeTime(
    t: Instant?,
    now: Instant = Instant.now(),
    lang: Language = CurrentLanguage.get(),
): String {
    if (t == null) return "—"
    val seconds = Duration.between(t, now).seconds.coerceAtLeast(0)
    return when {
        seconds < 60 -> UiText.Time.juuriNyt.get(lang)
        seconds < 60 * 60 -> UiText.Time.minuuttiaSitten(seconds / 60).get(lang)
        seconds < 60 * 60 * 24 -> UiText.Time.tuntiaSitten(seconds / 3_600).get(lang)
        seconds < 60 * 60 * 24 * 2 -> UiText.Time.eilen.get(lang)
        seconds < 60 * 60 * 24 * 7 -> UiText.Time.paivaaSitten(seconds / 86_400).get(lang)
        else -> t.atZone(TimeService.zoneId).toLocalDate().finnishDate()
    }
}
