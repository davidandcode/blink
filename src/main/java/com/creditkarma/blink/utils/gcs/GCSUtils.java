package com.creditkarma.blink.utils.gcs;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.*;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;

/**
 * Code cherry picked from Secor
 */
public class GCSUtils {

    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    /**
     * Global instance of the Storage. The best practice is to make it a single
     * globally shared instance across your application.
     */
    private static Storage mStorageService;

    public static Storage getService(String credentialsPath, int connectTimeoutMs, int readTimeoutMs) throws Exception {
        if (mStorageService == null) {
            HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();

            GoogleCredential credential;
            try {
                // Lookup if configured path from the properties; otherwise fallback to Google Application default
                if (credentialsPath != null && !credentialsPath.isEmpty()) {
                    credential = GoogleCredential
                            .fromStream(new FileInputStream(credentialsPath), httpTransport, JSON_FACTORY)
                            .createScoped(Collections.singleton(StorageScopes.CLOUD_PLATFORM));
                } else {
                    credential = GoogleCredential.getApplicationDefault(httpTransport, JSON_FACTORY);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to load Google credentials : " + credentialsPath, e);
            }

            mStorageService = new Storage.Builder(httpTransport, JSON_FACTORY,
                    setHttpBackoffTimeout(credential, connectTimeoutMs, readTimeoutMs))
                    .setApplicationName("com.creditkarma.logx")
                    .build();
        }
        return mStorageService;
    }
    private static HttpRequestInitializer setHttpBackoffTimeout(final HttpRequestInitializer requestInitializer,
                                                                final int connectTimeoutMs, final int readTimeoutMs) {
        return new HttpRequestInitializer() {
            @Override
            public void initialize(HttpRequest httpRequest) throws IOException {
                requestInitializer.initialize(httpRequest);

                // Configure exponential backoff on error
                // https://developers.google.com/api-client-library/java/google-http-java-client/backoff
                ExponentialBackOff backoff = new ExponentialBackOff();
                HttpUnsuccessfulResponseHandler backoffHandler = new HttpBackOffUnsuccessfulResponseHandler(backoff)
                        .setBackOffRequired(HttpBackOffUnsuccessfulResponseHandler.BackOffRequired.ALWAYS);
                httpRequest.setUnsuccessfulResponseHandler(backoffHandler);

                httpRequest.setConnectTimeout(connectTimeoutMs);
                httpRequest.setReadTimeout(readTimeoutMs);
            }
        };
    }
}
