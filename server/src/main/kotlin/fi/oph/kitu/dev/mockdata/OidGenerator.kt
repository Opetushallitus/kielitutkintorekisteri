package fi.oph.kitu.dev.mockdata

import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oid.OidClass
import fi.oph.kitu.util.result.getOrThrow
import kotlin.random.Random

fun createOid(
    oidClass: OidClass,
    n: Long,
): Oid =
    Oid
        .parse("${oidClass.node}.${n.toString().padStart(11, '0')}")
        .getOrThrow()

fun generateOidOfClass(
    oidClass: OidClass,
    random: Random = Random,
): Oid = createOid(oidClass, (0..99999999999).random(random))

/**
 * Generates random user OID under node class 1.2.246.562.24.
 *
 * Note: Node class 24 is the correct class for users,
 * but these values should not be used in production.
 */
fun generateRandomOppijaOid(random: Random = Random): Oid = generateOidOfClass(OidClass.OPPIJA, random)

/**
 * Generates random oppija OID under node class 1.2.246.562.240.
 *
 * Note: Node class 240 is the correct class for users,
 * but these values should not be used in production.
 */
fun generateRandomUserOid(random: Random = Random): Oid = generateOidOfClass(OidClass.USER, random)

/**
 * Generates random user OID under node class 1.2.246.562.100.
 *
 * Note: Node class 10 is the correct class for organizations,
 * but these values should not be used in production.
 */
fun generateRandomOrganizationOid(random: Random = Random) = generateOidOfClass(OidClass.ORG, random)
