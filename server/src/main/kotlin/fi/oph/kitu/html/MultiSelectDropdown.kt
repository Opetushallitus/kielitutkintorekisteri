package fi.oph.kitu.html

import fi.oph.kitu.i18n.UiText
import kotlinx.html.FlowContent
import kotlinx.html.InputType
import kotlinx.html.details
import kotlinx.html.id
import kotlinx.html.label
import kotlinx.html.li
import kotlinx.html.p
import kotlinx.html.span
import kotlinx.html.summary
import kotlinx.html.ul

data class MultiSelectOption(
    val value: String,
    val label: String,
    val checked: Boolean,
)

fun FlowContent.multiSelectDropdown(
    name: String,
    labelText: String,
    options: List<MultiSelectOption>,
) {
    val otsikkoId = "$name-otsikko"
    val valitutId = "$name-valitut"
    val dropdownId = "$name-valinta"
    val tyhja = UiText.Filter.kaikki.toString()

    p {
        id = otsikkoId
        +labelText
    }
    details(classes = "dropdown monivalinta") {
        id = dropdownId
        testId(dropdownId)
        data("tyhja", tyhja)
        summary {
            attributes["aria-labelledby"] = "$otsikkoId $valitutId"
            span {
                id = valitutId
                +options.filter { it.checked }.joinToString(", ") { it.label }.ifEmpty { tyhja }
            }
        }
        ul {
            options.forEach { option ->
                li {
                    label {
                        input(type = InputType.checkBox, name = name, value = option.value, checked = option.checked)
                        +option.label
                    }
                }
            }
        }
    }

    javascript(
        """
        const valinta = document.getElementById("$dropdownId")
        const valitut = document.getElementById("$valitutId")
        valinta.addEventListener("change", () => {
            const nimet = [...valinta.querySelectorAll("input:checked")]
                .map(input => input.parentElement.textContent.trim())
            valitut.textContent = nimet.join(", ") || valinta.dataset.tyhja
        })
        valinta.addEventListener("toggle", () => {
            if (valinta.open) valinta.querySelector("ul").scrollIntoView({ block: "nearest" })
        })
        """.trimIndent(),
    )
}
