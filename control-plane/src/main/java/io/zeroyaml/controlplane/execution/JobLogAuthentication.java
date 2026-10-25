package io.zeroyaml.controlplane.execution;

import java.security.cert.X509Certificate;

import javax.net.ssl.SSLPeerUnverifiedException;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/** Binds the CA-verified client certificate URI SAN to a Runner identity. */
class JobLogAuthentication implements ServerInterceptor {
	static final Context.Key<String> RUNNER_ID = Context.key("job-log-runner-id");
	private static final String IDENTITY_PREFIX = "urn:zeroyaml:runner:";

	@Override
	public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
			Metadata headers, ServerCallHandler<ReqT, RespT> next) {
		var session = call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
		try {
			if (session != null && session.getPeerCertificates()[0] instanceof X509Certificate certificate) {
				var names = certificate.getSubjectAlternativeNames();
				if (names != null) {
					var identities = names.stream().filter(name -> Integer.valueOf(6).equals(name.getFirst()))
							.map(name -> name.get(1).toString()).filter(name -> name.startsWith(IDENTITY_PREFIX))
							.map(name -> name.substring(IDENTITY_PREFIX.length())).toList();
					// Exactly one identity avoids ambiguous certificate attribution.
					if (identities.size() == 1 && !identities.getFirst().isBlank() && identities.getFirst().length() <= 256) {
						return Contexts.interceptCall(Context.current().withValue(RUNNER_ID, identities.getFirst()), call, headers, next);
					}
				}
			}
		} catch (SSLPeerUnverifiedException | java.security.cert.CertificateParsingException exception) {
			// Certificate details and request payloads never enter diagnostics.
		}
		call.close(Status.UNAUTHENTICATED.withDescription("Runner certificate identity required"), new Metadata());
		return new ServerCall.Listener<>() { };
	}
}
