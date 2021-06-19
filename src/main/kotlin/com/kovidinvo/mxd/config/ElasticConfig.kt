package com.kovidinvo.mxd.config

import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.http.ssl.SSLContextBuilder
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestClientBuilder
import org.elasticsearch.client.RestHighLevelClient
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.ClassPathResource
import org.springframework.data.elasticsearch.client.ClientConfiguration
import org.springframework.data.elasticsearch.client.RestClients
import org.springframework.data.elasticsearch.config.AbstractElasticsearchConfiguration
import java.net.URI
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.util.function.Function
import javax.net.ssl.SSLContext

@Configuration
class MxdEsRestClientConfig : AbstractElasticsearchConfiguration() {
    private val logger = LoggerFactory.getLogger(MxdEsRestClientConfig::class.java)

    @Value("\${mxd.elastic.host}")
    private lateinit var elasticHost : String

    @Value("\${mxd.elastic.username}")
    private lateinit var elasticUser : String

    @Value("\${mxd.elastic.password}")
    private lateinit var elasticPass : String


    override fun elasticsearchClient(): RestHighLevelClient {
        val certFact = CertificateFactory.getInstance("X.509")
        val caCert = certFact.generateCertificate(ClassPathResource("root-ca.pem").inputStream)
        val keyStore = KeyStore.getInstance("pkcs12")
        keyStore.load(null,null)
        keyStore.setCertificateEntry("ca",caCert)
        val sslCtx = SSLContextBuilder.create()
            .loadTrustMaterial(keyStore,null)
            .build()

        val client = RestHighLevelClient(RestClient.builder(
            HttpHost(elasticHost,9200,"https")
            )
            .setHttpClientConfigCallback { b ->
                b.setSSLContext(sslCtx)
                b.setSSLHostnameVerifier { s, sslSession -> true }
                val prov = BasicCredentialsProvider()
                prov.setCredentials(AuthScope.ANY,UsernamePasswordCredentials(elasticUser,elasticPass))
                b.setDefaultCredentialsProvider(prov)
                b
            }
        )

        return client
    }
}

