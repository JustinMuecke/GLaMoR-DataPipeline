package org.example;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.formats.*;

import java.io.File;

public class Main {



    private static OWLOntology removeAnnotationAxioms(OWLOntology ontology, OWLOntologyManager manager) {
        ontology.axioms().filter(OWLAxiom::isAnnotationAxiom).forEach(axiom -> manager.removeAxiom(ontology, axiom));
        return ontology;
    }

    /**
     *
     * @param ontology
     * @param manager
     * @return
     */
    private static OWLOntology removeSKOSAxioms(OWLOntology ontology, OWLOntologyManager manager) {
        IRI skosIRI = IRI.create("http://www.w3.org/2004/02/skos/core#");
        ontology.axioms()
                .filter(axiom -> axiom.signature()
                        .anyMatch(entity -> entity.getIRI().getNamespace().equals(skosIRI.toString())))
                .forEach(axiom -> manager.removeAxiom(ontology, axiom));
        return ontology;
    }

    public static void main(String[] args) throws OWLOntologyStorageException {
        System.out.println("Hello world!");
        File[] dir = new File("../data/ont_modules/").listFiles();

        for(File file : dir){
            if(file.getName().equals(".gitkeep")) continue;
            OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
            OWLOntology ontology = null;
            try {
                ontology = manager.loadOntologyFromOntologyDocument(file);
            } catch (OWLOntologyCreationException e) {
                throw new RuntimeException(e);
            }
            if(ontology == null) continue;
            // Remove Annotation Axioms
            ontology = removeAnnotationAxioms(ontology, manager);
            ontology = removeSKOSAxioms(ontology, manager);

            File output_file = new File("/home/foo/bar.owl");
            ManchesterSyntaxDocumentFormat manchesterSyntaxDocumentFormat = new ManchesterSyntaxDocumentFormat();
            manager.saveOntology(ontology, manchesterSyntaxDocumentFormat, IRI.create(new File("path/to/saved-ontology.manchester.owl").toURI()));
        }
    }

}