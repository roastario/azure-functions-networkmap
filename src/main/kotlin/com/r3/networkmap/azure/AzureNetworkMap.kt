package com.r3.networkmap.azure

import com.microsoft.azure.serverless.functions.HttpRequestMessage
import com.microsoft.azure.serverless.functions.HttpResponseMessage
import com.microsoft.azure.serverless.functions.OutputBinding
import com.microsoft.azure.serverless.functions.annotation.*
import net.corda.core.crypto.Crypto
import net.corda.core.internal.signWithCert
import net.corda.core.node.NetworkParameters
import net.corda.core.serialization.SerializedBytes
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.internal.SerializationEnvironmentImpl
import net.corda.core.serialization.internal.nodeSerializationEnv
import net.corda.core.utilities.base58ToByteArray
import net.corda.core.utilities.toBase58
import net.corda.nodeapi.internal.DEV_ROOT_CA
import net.corda.nodeapi.internal.SignedNodeInfo
import net.corda.nodeapi.internal.crypto.CertificateAndKeyPair
import net.corda.nodeapi.internal.crypto.CertificateType
import net.corda.nodeapi.internal.crypto.X509Utilities
import net.corda.nodeapi.internal.network.NetworkMap
import net.corda.nodeapi.internal.serialization.AMQP_P2P_CONTEXT
import net.corda.nodeapi.internal.serialization.AMQP_STORAGE_CONTEXT
import net.corda.nodeapi.internal.serialization.AllWhitelist
import net.corda.nodeapi.internal.serialization.SerializationFactoryImpl
import net.corda.nodeapi.internal.serialization.amqp.AMQPServerSerializationScheme
import net.corda.nodeapi.internal.serialization.amqp.DeserializationInput
import net.corda.nodeapi.internal.serialization.amqp.SerializerFactory
import java.security.KeyPair
import java.security.cert.X509Certificate
import java.time.Instant
import javax.security.auth.x500.X500Principal

class AzureNetworkMap {

    var networkMapCert: X509Certificate? = null;
    var keyPair: KeyPair? = null

    companion object {
        private val stubNetworkParameters = NetworkParameters(1, emptyList(), 10485760, Int.MAX_VALUE, Instant.now(), 10, emptyMap())
    }

    init {
        if (networkMapCert == null && keyPair == null) {
            val networkMapCa = createDevNetworkMapCa()
            keyPair = networkMapCa.keyPair
            networkMapCert = networkMapCa.certificate;
        }

        if (nodeSerializationEnv == null) {
            val classloader = this.javaClass.classLoader
            nodeSerializationEnv = SerializationEnvironmentImpl(
                    SerializationFactoryImpl().apply {
                        registerScheme(AMQPServerSerializationScheme(emptyList()))
                    },
                    p2pContext = AMQP_P2P_CONTEXT.withClassLoader(classloader),
                    rpcServerContext = AMQP_P2P_CONTEXT.withClassLoader(classloader),
                    storageContext = AMQP_STORAGE_CONTEXT.withClassLoader(classloader),
                    checkpointContext = AMQP_P2P_CONTEXT.withClassLoader(classloader)
            )
        }
    }


    @FunctionName("ping")
    fun ping(@HttpTrigger(
            name = "input", methods = ["get"],
            authLevel = AuthorizationLevel.ANONYMOUS,
            dataType = "binary",
            route = "ping")
             req: HttpRequestMessage<Any>): ByteArray {
        return "OK".toByteArray()
    }

    @FunctionName("publishNodeInfo")
    fun postNodeInfo(@HttpTrigger(
            name = "input", methods = ["post"],
            authLevel = AuthorizationLevel.ANONYMOUS,
            dataType = "binary",
            route = "network-map/publish")
                     input: ByteArray,
                     @QueueOutput(queueName = "nodeinfos", connection = "AzureWebJobsStorage", name = "nodeinfos") queue: OutputBinding<ByteArray>): String {
        val factory = SerializerFactory(AllWhitelist, ClassLoader.getSystemClassLoader())
        val signedNodeInfo = DeserializationInput(factory).deserialize(SerializedBytes<SignedNodeInfo>(input))
        signedNodeInfo.verified()
        queue.value = input
        return "ok";
    }

    @FunctionName("getNetworkMap")
    @HttpOutput(name = "\$return", dataType = "binary")
    fun getNetworkMapArray(
            @HttpTrigger(methods = ["get"],
                    dataType = "binary",
                    authLevel = AuthorizationLevel.ANONYMOUS,
                    route = "network-map",
                    name = "in") token: HttpRequestMessage<String>,
            @BlobInput(name = "serialisedNetworkMap",
                    dataType = "binary",
                    connection = "AzureWebJobsStorage",
                    path = "networkmap/network-map.ser") networkMap: ByteArray): HttpResponseMessage<ByteArray> {
        return token.createResponse(200, networkMap)
    }

    @FunctionName("consumeNodeInfo")
    fun consumeNodeInfoFromQueue(@QueueTrigger(queueName = "nodeinfos", connection = "AzureWebJobsStorage", name = "nodeinfos", dataType = "binary") addedNodeInfo: ByteArray,
                                 @TableOutput(name = "networkmap", tableName = "networkmap", connection = "AzureWebJobsStorage", partitionKey = "nodeInfos") table: OutputBinding<SignedNodeInfoRow>) {
        val nodeInfo = addedNodeInfo.deserialize<SignedNodeInfo>()
        table.value = SignedNodeInfoRow(nodeInfo.raw.hash.toString(), addedNodeInfo.toBase58())
    }

    @FunctionName("getNodeInfo")
    fun getNodeInfo(@HttpTrigger(methods = ["get"], authLevel = AuthorizationLevel.ANONYMOUS, route = "network-map/node-info/{hash}", name = "token")
                    @BindingName("hash") hash: String?,
                    @TableInput(name = "networkmap", tableName = "networkmap", connection = "AzureWebJobsStorage", partitionKey = "nodeInfos", rowKey = "{hash}") tableContents: Map<String, String>?): ByteArray {
        return tableContents?.get(SignedNodeInfoRow::arrayAsBase58String.name)?.base58ToByteArray()
                ?: throw IllegalStateException("no previously submitted node for hash: $hash")
    }

    @FunctionName("scheduledNetworkMapBuild")
    fun triggerNetworkMapBuild(@TimerTrigger(name = "scheduledBuild", schedule = "0 */59 * * * *")
                               @BindingName("scheduledBuild") triggerData: String?,
                               @TableInput(name = "networkmap", tableName = "networkmap", connection = "AzureWebJobsStorage", partitionKey = "nodeInfos") tableContents: List<Map<String, String>>?,
                               @BlobOutput(name = "networkMapStoreOut", dataType = "binary", path = "networkmap/network-map.ser", connection = "AzureWebJobsStorage") outputBinding: OutputBinding<ByteArray>) {
        buildNetworkMap(triggerData, tableContents, outputBinding)
    }

    @FunctionName("generateNetworkMap")
    fun buildNetworkMap(@HttpTrigger(name = "generateTrigger", methods = ["get"], authLevel = AuthorizationLevel.ANONYMOUS, route = "generate") triggerData: String?,
                        @TableInput(name = "networkmap", tableName = "networkmap", connection = "AzureWebJobsStorage", partitionKey = "nodeInfos") tableContents: List<Map<String, String>>?,
                        @BlobOutput(name = "networkMapStoreOut", dataType = "binary", path = "networkmap/network-map.ser", connection = "AzureWebJobsStorage") outputBinding: OutputBinding<ByteArray>): String {

        println("Generating network map due to: $triggerData")
        tableContents?.let {
            val hashes = tableContents.map { it[SignedNodeInfoRow::arrayAsBase58String.name] }
                    .map { it!!.base58ToByteArray() }
                    .map { it.deserialize<SignedNodeInfo>() }
                    .map { it.raw.hash }
            val signedNetParams = stubNetworkParameters.signWithCert(keyPair!!.private, networkMapCert!!)
            val networkMap = NetworkMap(hashes, signedNetParams.raw.hash, null)
            val signedNetworkMap = networkMap.signWithCert(keyPair!!.private, networkMapCert!!)
            outputBinding.value = signedNetworkMap.raw.bytes
        }
        return "OK";
    }

}


data class SignedNodeInfoRow(val RowKey: String, val arrayAsBase58String: String)

fun createDevNetworkMapCa(rootCa: CertificateAndKeyPair = DEV_ROOT_CA): CertificateAndKeyPair {
    val keyPair = Crypto.generateKeyPair()
    val cert = X509Utilities.createCertificate(
            CertificateType.NETWORK_MAP,
            rootCa.certificate,
            rootCa.keyPair,
            X500Principal("CN=Network Map,O=R3 Ltd,L=London,C=GB"),
            keyPair.public)
    return CertificateAndKeyPair(cert, keyPair)
}



