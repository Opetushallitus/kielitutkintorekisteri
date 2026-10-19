package fi.oph.kitu.csv

import java.io.OutputStream

private val UTF_8_BOM = byteArrayOf(0xEF.toByte(), 0xBB.toByte(), 0xBF.toByte())

// Excel detects UTF-8 reliably only when the file starts with a BOM and contains no `sep=`
// directive; with `sep=` present, several Excel versions skip BOM-based charset detection and
// fall back to the system ANSI codepage (Windows-1252 on most installs), mangling ä/ö/é/…
// We therefore drop `sep=` and use `;` as the field separator everywhere — that matches the
// list-separator default of fi-FI (and most other European) Excel installs, so columns split
// correctly without the directive. (Trade-off: en-US Excel will collapse rows into one cell.)
fun OutputStream.writeExcelCsvPrelude() {
    write(UTF_8_BOM)
}
