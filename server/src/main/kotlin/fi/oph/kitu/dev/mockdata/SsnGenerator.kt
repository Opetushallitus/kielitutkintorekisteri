package fi.oph.kitu.dev.mockdata

import fi.oph.kitu.yki.Sukupuoli
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import kotlin.random.Random

fun generateRandomSsn(
    min: LocalDate = LocalDate.of(1900, 1, 1),
    max: LocalDate = LocalDate.now(),
    sex: Sukupuoli = listOf(Sukupuoli.M, Sukupuoli.N).random(),
    isTemporary: Boolean = true,
    random: Random = Random,
): String {
    val birthday = getRandomLocalDate(min, max, random)
    val bdayString = birthday.format(DateTimeFormatter.ofPattern("ddMMyy"))
    val separator = birthday.getSeparator()
    val id = getSsnId(birthday, sex, isTemporary)

    val checksum = getSsnChecksum(bdayString, id)

    return "$bdayString$separator$id$checksum"
}

private fun LocalDate.getSeparator(): String =
    when {
        this.isAfter(LocalDate.of(2100, 1, 1))
        -> "B"

        this in (LocalDate.of(2000, 1, 1))..LocalDate.of(2100, 1, 1)
        -> "A"

        this in LocalDate.of(1900, 1, 1)..LocalDate.of(2000, 1, 1)
        -> "-"

        else
        -> "+"
    }

private fun getSsnId(
    birthday: LocalDate,
    sex: Sukupuoli,
    isTemporary: Boolean,
): String {
    val condition: (num: Int) -> Boolean = { num ->
        if (sex == Sukupuoli.N) {
            num % 2 != 0
        } else {
            num % 2 == 0
        }
    }

    val range =
        if (isTemporary) {
            (900..999)
        } else {
            (2..899)
        }

    var number: Int
    do {
        number = range.random()
    } while (condition(number))
    return number.toString().padStart(3, '0')
}

private fun getSsnChecksum(
    bdayString: String,
    id: String,
): String =
    @Suppress("ktlint:standard:argument-list-wrapping")
    listOf(
        "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
        "A", "B", "C", "D", "E", "F", "H", "J", "K", "L",
        "M", "N", "P", "R", "S", "T", "U", "V", "W", "X", "Y",
    ).getOrElse(
        "$bdayString$id"
            .toInt()
            .rem(31),
    ) { throw IllegalArgumentException("Bad checksum: $it") }
