package sparqldemo;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.http.HTTPRepository;

public class SparqlMain {

	private String rdf4jServer = "http://localhost:7200/";
	private String repositoryID = "DefaultRepo";
	private Repository repo;
	private RepositoryConnection repoConn;

	public SparqlMain() {
		this.repo = getRepositoryFromURL(rdf4jServer, repositoryID);
		this.repoConn = repo.getConnection();

		showResults(getQuerry().toString());
		showResults2(getAllLabelsAndCodes().toString());

	}

	public Repository getRepositoryFromURL(String repoID, String repoURL) throws RepositoryException {
		System.out.println("we in");
		Repository repo = new HTTPRepository(rdf4jServer, repositoryID);
		repo.initialize();

		return repo;
	}

	public static void main(String[] args) {
		new SparqlMain();
	}

	public StringBuilder getQuerry() {
		StringBuilder queryString = new StringBuilder("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>")
				.append("PREFIX bud: <http://data.europa.eu/bud#>")
				.append("select ?s ?p where {")
				.append(" ?s rdf:type bud:Catpol .")
				.append("?s bud:code ?p }");

		return queryString;
	}

	public StringBuilder getAllLabelsAndCodes() {
		StringBuilder queryString = new StringBuilder("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>")
				.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>")
				.append("PREFIX bud: <http://data.europa.eu/bud#>")
				.append("select ?label ?code where {")
				.append(" ?concept rdf:type bud:Catpol .")
				.append(" ?concept rdfs:label ?label .")
				.append(" ?concept bud:code ?code .")
				.append("FILTER langMatches( lang(?label), \"EN\" )}");

		return queryString;
	}

	public void showResults(String query) {
		Map<String, String> resultMap = new HashMap<>();

		try {
			TupleQuery tupleQuery = repoConn.prepareTupleQuery(QueryLanguage.SPARQL, query);
			TupleQueryResult result = null;
			
			System.out.println("----- WE GOT HERE 1 -----");


			try {
				result = tupleQuery.evaluate();
				while (result.hasNext()) {
					BindingSet bindingSet = result.next();
					IRI valueOfS = (IRI) bindingSet.getValue("s");
					Literal valueOfP = (Literal) bindingSet.getValue("p");

					System.out.println("----- WE GOT HERE 2 -----");

					resultMap.put(valueOfP.stringValue(), valueOfS.stringValue());
				}

				System.out.println("----- QUERRY RESULTS -----");
				resultMap.forEach((key, value) -> System.out.println("Key: " + key + "\t" + " Value: " + value));

			} catch (QueryEvaluationException qevalex1) {
				System.out.println("----- ERRO A AVALIAR A QUERRY 1 -----");
			} finally {
				if (result != null) {
					try {

					} catch (QueryEvaluationException qevalex2) {
						System.out.println(qevalex2.getMessage());
					}
				}
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			try {
				repoConn.close();
			} catch (RepositoryException repex) {
				System.out.println(repex.getMessage());
			}
		}

	}
	
	public void showResults2(String query) {
		Map<String, String> resultMap = new HashMap<>();

		try {
			TupleQuery tupleQuery = repoConn.prepareTupleQuery(QueryLanguage.SPARQL, query);
			TupleQueryResult result = null;
			
			System.out.println("----- WE GOT HERE 1 -----");


			try {
				result = tupleQuery.evaluate();
				while (result.hasNext()) {
					BindingSet bindingSet = result.next();
					Literal valueOfS = (Literal) bindingSet.getValue("label");
					Literal valueOfP = (Literal) bindingSet.getValue("code");

					resultMap.put(valueOfP.stringValue(), valueOfS.stringValue());
				}

				System.out.println("----- QUERRY RESULTS -----");
				resultMap.forEach((key, value) -> System.out.println("Key: " + key + "\t" + " Value: " + value));

			} catch (QueryEvaluationException qevalex1) {
				System.out.println("----- ERRO A AVALIAR A QUERRY 1 -----");
			} finally {
				if (result != null) {
					try {

					} catch (QueryEvaluationException qevalex2) {
						System.out.println(qevalex2.getMessage());
					}
				}
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			try {
				repoConn.close();
			} catch (RepositoryException repex) {
				System.out.println(repex.getMessage());
			}
		}

	}

}
