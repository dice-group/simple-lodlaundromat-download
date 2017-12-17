package org.dice_group.lod.down;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Iterator;

import org.aksw.jena_sparql_api.core.QueryExecutionFactory;
import org.aksw.jena_sparql_api.delay.core.QueryExecutionFactoryDelay;
import org.aksw.jena_sparql_api.http.QueryExecutionFactoryHttp;
import org.aksw.jena_sparql_api.pagination.core.QueryExecutionFactoryPaginated;
import org.apache.commons.io.IOUtils;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListRequester implements Iterator<DatasetMetaData>, Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ListRequester.class);

    public static final String DEFAULT_ENDPOINT = "";
    private static final long DELAY = 1000;
    private static final String DEFAULT_GRAPH = "http://lodlaundromat.org#12";
    public static final String SPARQL_QUERY_RESOURCE = "lod-meta-data.query";

    public static ListRequester create(String endpoint) {
        // Check whether we can be sure that it is a SPARQL endpoint
        QueryExecutionFactory qef = null;
        QueryExecution execution = null;
        String query = null;
        try {
            query = IOUtils.toString(ListRequester.class.getResourceAsStream(SPARQL_QUERY_RESOURCE),
                    StandardCharsets.UTF_8);
            if (query == null) {
                LOGGER.error("Couldn't load SPARQL query for getting meta data. Returning null.");
                return null;
            }
        } catch (Throwable e) {
            LOGGER.error("Couldn't load SPARQL query for getting meta data. Returning null.", e);
            return null;
        }
        try {
            // Create query execution instance
            qef = initQueryExecution(endpoint);
            execution = qef.createQueryExecution(query);
            ResultSet resultSet = execution.execSelect();
            return new ListRequester(qef, execution, resultSet);
        } catch (Throwable e) {
            LOGGER.error("Couldn't create QueryExecutionFactory for \"" + endpoint + "\". Returning null.");
            if (execution != null) {
                execution.close();
            }
            if (qef != null) {
                try {
                    qef.close();
                } catch (Exception e1) {
                }
            }
            return null;
        }
    }

    protected static QueryExecutionFactory initQueryExecution(String uri) throws ClassNotFoundException, SQLException {
        QueryExecutionFactory qef;
        qef = new QueryExecutionFactoryHttp(uri, DEFAULT_GRAPH);
        qef = new QueryExecutionFactoryDelay(qef, DELAY);
        try {
            return new QueryExecutionFactoryPaginated(qef, 100);
        } catch (Exception e) {
            LOGGER.info("Couldn't create Factory with pagination. Returning Factory without pagination. Exception: {}",
                    e.getLocalizedMessage());
            return qef;
        }
    }

    private QueryExecutionFactory qef;
    private QueryExecution execution;
    private ResultSet resultSet;

    protected ListRequester(QueryExecutionFactory qef, QueryExecution execution, ResultSet resultSet) {
        this.qef = qef;
        this.execution = execution;
        this.resultSet = resultSet;
    }

    @Override
    public boolean hasNext() {
        return resultSet.hasNext();
    }

    @Override
    public DatasetMetaData next() {
        QuerySolution qs = resultSet.next();
        return new DatasetMetaData(qs.get("datadoc"), qs.get("md5"), qs.get("url"), qs.get("triples"),
                qs.get("parent"));
    }

    @Override
    public void close() throws IOException {
        if (execution != null) {
            execution.close();
        }
        if (qef != null) {
            try {
                qef.close();
            } catch (Exception e) {
            }
        }
    }
}
