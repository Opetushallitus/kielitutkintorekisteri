package fi.oph.kitu.html

import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText

fun ohjeUrl(sivu: LocalizedString?): String =
    sivu?.toString()?.trim()?.takeIf { it.onUrl() }
        ?: UiText.Ohje.oletus
            .toString()
            .trim()

private fun String.onUrl(): Boolean = startsWith("https://") || startsWith("http://")
