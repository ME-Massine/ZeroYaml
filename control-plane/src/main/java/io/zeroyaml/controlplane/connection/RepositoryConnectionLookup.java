package io.zeroyaml.controlplane.connection;

import java.util.Optional;

import io.zeroyaml.controlplane.domain.repository.RepositoryConnection;
import io.zeroyaml.controlplane.domain.repository.RepositoryIdentity;

/**
 * Read-only view of the connected repositories, for callers that only need to
 * know whether a repository is connected and in which status.
 */
public interface RepositoryConnectionLookup {

	/**
	 * Returns the recorded connection for {@code identity}, or empty when the
	 * repository is not connected. The result is a detached snapshot.
	 */
	Optional<RepositoryConnection> find(RepositoryIdentity identity);
}
