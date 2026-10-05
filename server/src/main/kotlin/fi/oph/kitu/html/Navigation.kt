package fi.oph.kitu.html

import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.webmvc.Links

object Navigation {
    val mainNavigation get() =
        listOf(
            MenuItemGroup(
                "yki",
                UiText.Nav.yki,
                listOf(
                    MenuItem(UiText.Nav.suoritukset, Links.Yki.suoritukset()),
                    MenuItem(UiText.Nav.arvioijat, Links.Yki.arvioijat()),
                    MenuItem(UiText.Nav.tarkistusarvioinnit, Links.Yki.tarkistusArvioinnit()),
                ),
            ),
            MenuItemGroup(
                "koto-kielitesti",
                UiText.Nav.kotoutumiskoulutuksenPaattotesti,
                listOf(
                    MenuItem(UiText.Nav.suoritukset, Links.Kielitesti.suoritukset()),
                    MenuItem(UiText.Nav.tehtavapaketit, Links.Tehtavapankki.list()),
                ),
            ),
            MenuItemGroup(
                "vkt",
                UiText.Nav.vkt,
                listOf(
                    MenuItem(UiText.Nav.kaikkiSuoritukset, Links.Vkt.suoritukset()),
                    MenuItem(
                        UiText.Nav.erinomaisenTaidonIlmoittautuneet,
                        Links.Vkt.erinomaisenTaitotasonIlmoittautuneet(),
                    ),
                    MenuItem(
                        UiText.Nav.erinomaisenTaidonSuoritukset,
                        Links.Vkt.erinomaisenTaitotasonArvioidutSuoritukset(),
                    ),
                    MenuItem(
                        UiText.Nav.hyvanJaTyydyttavanSuoritukset,
                        Links.Vkt.hyvanJaTyydyttavanTaitotasonSuoritukset(),
                    ),
                ),
            ),
            MenuItemGroup(
                "admin",
                UiText.Nav.yllapito,
                listOf(
                    MenuItem(
                        UiText.Nav.erajojenHallinta,
                        "/kielitutkinnot/db-scheduler",
                    ),
                ),
            ),
        )

    data class MenuItemGroup(
        val id: String,
        val name: LocalizedString,
        val children: List<MenuItem>,
    )

    data class MenuItem(
        val title: LocalizedString,
        val ref: String?,
        val current: Boolean = false,
    ) {
        constructor(title: String, ref: String?, current: Boolean = false) :
            this(LocalizedString(fi = title), ref, current)
    }

    fun List<MenuItem>.setCurrentItem(ref: String?) =
        if (ref != null) {
            this.map { it.copy(current = it.ref == ref) }
        } else {
            this
        }

    fun List<MenuItemGroup>.flatten(): List<MenuItem> = this.flatMap { listOf(MenuItem(it.name, null)) + it.children }
}
