package fi.oph.kitu.i18n

import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import kotlin.reflect.full.declaredMemberFunctions
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.jvm.isAccessible

/**
 * Käy käynnistyksessä läpi kaikki [UiText]-merkinnät, jotta [UiTextRegistry] sisältää kaikki avaimet
 * heti bootin jälkeen (avaimet eivät ole luettavissa reflektiolla arvoista, joten ne kirjautuvat
 * [UiText]-tekstien resolvoinnin yhteydessä).
 *
 * Rekisterin täydellisyys on Tolgee-synkronoinnin ehto: lukematta jäänyt merkintä näyttäisi
 * synkronoinnille avaimelta, jota ei enää ole koodissa, ja se poistettaisiin Tolgeesta.
 * Siksi jokainen [LocalizedString]ia palauttava jäsen on pakko saada kutsutuksi, ja
 * epäonnistuminen kaataa käynnistyksen — myös testeissä ja paikallisesti.
 *
 * Lisäksi tarkistetaan että jokaiselle avaimelle löytyy lähdeteksti luokkapolulta. Ilman sitä
 * käyttöliittymään renderöityisi avainmerkkijonoja, ja Tolgeeseen työnnettäisiin avain ilman
 * lähdetekstiä.
 */
@Component
@Order(0)
class UiTextWarmup : ApplicationRunner {
    override fun run(args: ApplicationArguments) {
        val virheet = warmUp(UiText)
        if (virheet.isNotEmpty()) {
            error("UiText-avaimia ei saatu luettua: ${virheet.joinToString("; ")}")
        }

        val puuttuvat = UiTextRegistry.all() - TolgeeMessages.sourceKeys()
        if (puuttuvat.isNotEmpty()) {
            error(
                "UiText-avaimia puuttuu lähdeteksteistä ($KAANNOSHAKEMISTO/fi.json): " +
                    puuttuvat.sorted().joinToString(", "),
            )
        }
    }

    internal fun warmUp(obj: Any): List<String> {
        val virheet = mutableListOf<String>()
        val nimi = obj::class.qualifiedName ?: obj::class.simpleName

        obj::class
            .declaredMemberProperties
            .filter { it.returnType.classifier == LocalizedString::class }
            .forEach { property ->
                property.isAccessible = true
                runCatching { property.getter.call(obj) }
                    .onFailure { virheet += "$nimi.${property.name}: ${it.message}" }
            }

        obj::class
            .declaredMemberFunctions
            .filter { it.returnType.classifier == LocalizedString::class }
            .forEach { function ->
                val parametrit = function.parameters.drop(1)
                val tuntematonParametri = parametrit.firstOrNull { it.type.classifier != Long::class }
                if (tuntematonParametri != null) {
                    virheet += "$nimi.${function.name}: tuntematon parametrityyppi ${tuntematonParametri.type}"
                    return@forEach
                }
                function.isAccessible = true
                val longArgs = Array<Any?>(parametrit.size) { 0L }
                runCatching { function.call(obj, *longArgs) }
                    .onFailure { virheet += "$nimi.${function.name}: ${it.message}" }
            }

        obj::class
            .nestedClasses
            .mapNotNull { it.objectInstance }
            .forEach { virheet += warmUp(it) }

        return virheet
    }
}
