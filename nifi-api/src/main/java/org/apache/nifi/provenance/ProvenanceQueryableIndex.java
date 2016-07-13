package org.apache.nifi.provenance;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QuerySubmission;
import org.apache.nifi.provenance.search.SearchableField;

import java.util.List;

/**
 * Created by apiri on 11Jul2016.
 */
public interface ProvenanceQueryableIndex {
    /**
     * Submits an asynchronous request to process the given query, returning an
     * identifier that can be used to fetch the results at a later time
     *
     * @param query to submit
     * @param user the NiFi User to authorize the events against
     *
     * @return an identifier that can be used to fetch the results at a later
     *         time
     */
    QuerySubmission submitQuery(Query query, NiFiUser user);

    /**
     * @param queryIdentifier of the query
     * @param user the user who is retrieving the query
     *
     * @return the QueryResult associated with the given identifier, if the
     *         query has finished processing. If the query has not yet finished running,
     *         returns <code>null</code>
     */
    QuerySubmission retrieveQuerySubmission(String queryIdentifier, NiFiUser user);

    /**
     * Submits a Lineage Computation to be completed and returns the
     * AsynchronousLineageResult that indicates the status of the request and
     * the results, if the computation is complete. If the given user does not
     * have authorization to view one of the events in the lineage, a placeholder
     * event will be used instead that provides none of the event details except
     * for the identifier of the component that emitted the Provenance Event. It is
     * necessary to include this node in the lineage view so that the lineage makes
     * sense, rather than showing disconnected graphs when the user is not authorized
     * for all components' provenance events.
     *
     * @param flowFileUuid the UUID of the FlowFile for which the Lineage should
     *            be calculated
     * @param user the NiFi User to authorize events against
     *
     * @return a {@link ComputeLineageSubmission} object that can be used to
     *         check if the computing is complete and if so get the results
     */
    ComputeLineageSubmission submitLineageComputation(String flowFileUuid, NiFiUser user);

    /**
     * @param lineageIdentifier identifier of lineage to compute
     * @param user the user who is retrieving the lineage submission
     *
     * @return the {@link ComputeLineageSubmission} associated with the given
     *         identifier
     */
    ComputeLineageSubmission retrieveLineageSubmission(String lineageIdentifier, NiFiUser user);

    /**
     * Submits a request to expand the parents of the event with the given id. If the given user
     * is not authorized to access any event, a placeholder will be used instead that contains only
     * the ID of the component that emitted the event.
     *
     * @param eventId the one-up id of the Event to expand
     * @param user the NiFi user to authorize events against
     * @return a submission which can be checked for status
     *
     * @throws IllegalArgumentException if the given identifier identifies a
     *             Provenance Event that has a Type that is not expandable or if the
     *             identifier cannot be found
     */
    ComputeLineageSubmission submitExpandParents(long eventId, NiFiUser user);

    /**
     * Submits a request to expand the children of the event with the given id. If the given user
     * is not authorized to access any event, a placeholder will be used instead that contains only
     * the ID of the component that emitted the event.
     *
     * @param eventId the one-up id of the Event
     * @param user the NiFi user to authorize events against
     *
     * @return a submission which can be checked for status
     *
     * @throws IllegalArgumentException if the given identifier identifies a
     *             Provenance Event that has a Type that is not expandable or if the
     *             identifier cannot be found
     */
    ComputeLineageSubmission submitExpandChildren(long eventId, NiFiUser user);

    /**
     * @return a list of all fields that can be searched via the
     * {@link ProvenanceQueryableIndex#submitQuery(Query, NiFiUser)} method
     */
    List<SearchableField> getSearchableFields();

    /**
     * @return a list of all FlowFile attributes that can be searched via the
     * {@link ProvenanceQueryableIndex#submitQuery(Query, NiFiUser)} method
     */
    List<SearchableField> getSearchableAttributes();
}
