package fi.oph.kitu.i18n

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.boot.DefaultApplicationArguments
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Huom: testikatalogien `record` ei kirjaa [UiTextRegistry]yn. Rekisteri on globaali ja
 * [UiTextWarmup.run] vertaa sitä lähdeteksteihin, joten synteettinen `test.*`-avain kaataisi
 * käynnistystarkistuksen kaikissa myöhemmissä testeissä.
 */
private val kutsutut = mutableMapOf<String, String>()

private fun record(
    key: String,
    fi: String,
): LocalizedString {
    kutsutut[key] = fi
    return LocalizedString.withTolgeeKey(key)
}

private object TestTexts {
    val pelkkaProperty: LocalizedString get() = record("test.pelkkaProperty", "arvo")

    fun ilmanParametreja(): LocalizedString = record("test.ilmanParametreja", "arvo")

    fun yksiParametri(count: Long) = record("test.yksiParametri", "count=$count")

    fun kaksiParametria(
        eka: Long,
        toka: Long,
    ) = record("test.kaksiParametria", "eka=$eka toka=$toka")

    object Sisakkainen {
        val teksti: LocalizedString get() = record("test.sisakkainen.teksti", "arvo")
    }
}

private object TestTextsVirheellinen {
    fun eiLongParametria(teksti: String) = record("test.eiLongParametria", teksti)
}

private object TestTextsHajoava {
    val rikki: LocalizedString get() = error("hajoaa tarkoituksella")
}

class UiTextWarmupTest {
    @AfterEach
    fun palautaLahdetekstit() = TolgeeMessages.reloadSourceTexts()

    @Test
    fun `warmup rekisteroi propertyt, parametrittomat ja Long-funktiot seka sisakkaiset objektit`() {
        kutsutut.clear()
        val virheet = UiTextWarmup().warmUp(TestTexts)

        assertEquals(emptyList(), virheet, "Kelvollisesta katalogista ei saa tulla virheitä")
        assertContains(kutsutut.keys, "test.pelkkaProperty", "Propertyn tulee tulla kutsutuksi")
        assertContains(kutsutut.keys, "test.ilmanParametreja", "Parametrittoman funktion tulee tulla kutsutuksi")
        assertContains(kutsutut.keys, "test.yksiParametri", "Yhden Long-parametrin funktion tulee tulla kutsutuksi")
        assertContains(kutsutut.keys, "test.kaksiParametria", "Kahden Long-parametrin funktion tulee tulla kutsutuksi")
        assertContains(kutsutut.keys, "test.sisakkainen.teksti", "Sisäkkäisen objektin tulee tulla kutsutuksi")
    }

    @Test
    fun `warmup kutsuu Long-parametrit nollilla`() {
        kutsutut.clear()
        UiTextWarmup().warmUp(TestTexts)

        assertEquals("count=0", kutsutut["test.yksiParametri"])
        assertEquals("eka=0 toka=0", kutsutut["test.kaksiParametria"])
    }

    @Test
    fun `warmup ilmoittaa virheen funktiosta jota ei voi kutsua`() {
        val virheet = UiTextWarmup().warmUp(TestTextsVirheellinen)

        assertEquals(1, virheet.size, "Kutsumaton funktio on virhe, ei hiljainen ohitus: $virheet")
        assertContains(virheet.single(), "eiLongParametria")
    }

    @Test
    fun `warmup ilmoittaa virheen getterista joka heittaa`() {
        val virheet = UiTextWarmup().warmUp(TestTextsHajoava)

        assertEquals(1, virheet.size, "Heittävä getteri on virhe: $virheet")
        assertContains(virheet.single(), "rikki")
    }

    @Test
    fun `warmup rekisteroi koko UiText-luettelon heittamatta`() {
        UiTextWarmup().run(DefaultApplicationArguments())

        assertTrue(
            UiTextRegistry.all().size > 400,
            "UiTextin avaimia pitäisi rekisteröityä satoja, saatiin ${UiTextRegistry.all().size}",
        )
    }

    @Test
    fun `puuttuva lahdeteksti kaataa kaynnistyksen`() {
        TolgeeMessages.setSourceTexts(mapOf("appTitle" to LocalizedString(fi = "Vain tämä")))

        val virhe =
            assertFailsWith<IllegalStateException> {
                UiTextWarmup().run(DefaultApplicationArguments())
            }

        assertContains(
            virhe.message.orEmpty(),
            "puuttuu lähdeteksteistä",
            message = "Käynnistyksen on kaaduttava selkeään virheeseen, ei renderöitävä avainmerkkijonoja",
        )
    }
}
