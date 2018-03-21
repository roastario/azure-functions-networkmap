package com.r3.networkmap.azure;

import com.microsoft.azure.serverless.functions.HttpRequestMessage;
import com.microsoft.azure.serverless.functions.HttpResponseMessage;
import com.microsoft.azure.serverless.functions.annotation.*;

import java.nio.ByteBuffer;

public class DelegatedToJavaFunctions {


    @FunctionName("getNetworkMapBA")
    @HttpOutput(name = "$return", dataType = "binary")
    public HttpResponseMessage<byte[]> getNetworkMapArray(
            @HttpTrigger(methods = {"get"}, dataType = "binary", authLevel = AuthorizationLevel.ANONYMOUS, route = "network-mapBA", name = "in") HttpRequestMessage<String> token,
            @BlobInput(name = "serialisedNetworkMap", dataType = "binary", connection = "AzureWebJobsStorage", path = "networkmap/network-map.ser") byte[] networkMap) {
        HttpResponseMessage<byte[]> response = token.createResponse(200, (networkMap));
//        response.addHeader("Content-Length", "${networkMap!!.size}");
//        response.addHeader("Content-Type", "application/octet-stream");
        return response;
    }

    @FunctionName("getNetworkMapBB")
    @HttpOutput(name = "$return", dataType = "binary")
    public HttpResponseMessage<ByteBuffer> getNetworkMapBuffer(
            @HttpTrigger(methods = {"get"}, dataType = "binary", authLevel = AuthorizationLevel.ANONYMOUS, route = "network-mapBB", name = "in") HttpRequestMessage<String> token,
            @BlobInput(name = "serialisedNetworkMap", dataType = "binary", connection = "AzureWebJobsStorage", path = "networkmap/network-map.ser") byte[] networkMap) {
        HttpResponseMessage<ByteBuffer> response = token.createResponse(200, ByteBuffer.wrap(networkMap));
//        response.addHeader("Content-Length", "${networkMap!!.size}");
//        response.addHeader("Content-Type", "application/octet-stream");
        return response;
    }

    @FunctionName("getNetworkMapBBNoWrapper")
    @HttpOutput(name = "$return", dataType = "binary")
    public ByteBuffer getNetworkMapBBNoWrapper(
            @HttpTrigger(methods = {"get"}, dataType = "binary", authLevel = AuthorizationLevel.ANONYMOUS, route = "network-mapBBNW", name = "in") HttpRequestMessage<String> token,
            @BlobInput(name = "serialisedNetworkMap", dataType = "binary", connection = "AzureWebJobsStorage", path = "networkmap/network-map.ser") byte[] networkMap) {
//        response.addHeader("Content-Length", "${networkMap!!.size}");
//        response.addHeader("Content-Type", "application/octet-stream");
        return ByteBuffer.wrap(networkMap);
    }

    @FunctionName("getNetworkMapBANoWrapper")
    @HttpOutput(name = "$return", dataType = "binary")
    public byte[] getNetworkMapBANoWrapper(
            @HttpTrigger(methods = {"get"}, dataType = "binary", authLevel = AuthorizationLevel.ANONYMOUS, route = "network-mapBANW", name = "in") HttpRequestMessage<String> token,
            @BlobInput(name = "serialisedNetworkMap", dataType = "binary", connection = "AzureWebJobsStorage", path = "networkmap/network-map.ser") byte[] networkMap) {
//        response.addHeader("Content-Length", "${networkMap!!.size}");
//        response.addHeader("Content-Type", "application/octet-stream");
        return (networkMap);
    }


}
