package org.example;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.formats.*;
import org.semanticweb.owlapi.util.DefaultPrefixManager;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class Main {



    private static void removeAnnotationAxioms(OWLOntology ontology, OWLOntologyManager manager) {
        ontology.axioms().filter(OWLAxiom::isAnnotationAxiom).forEach(axiom -> manager.removeAxiom(ontology, axiom));
    }

    /**
     *
     * @param ontology
     * @param manager
     * @return
     */
    private static void removeSKOSAxioms(OWLOntology ontology, OWLOntologyManager manager) {
        IRI skosIRI = IRI.create("http://www.w3.org/2004/02/skos/core#");
        ontology.axioms()
                .filter(axiom -> axiom.signature()
                        .anyMatch(entity -> entity.getIRI().getNamespace().equals(skosIRI.toString())))
                .forEach(axiom -> manager.removeAxiom(ontology, axiom));
    }

    private static Optional<Set<String>> findBaseIRI(OWLOntology ontology, OWLOntologyManager manager){
        Set<OWLEntity> entities = ontology.getSignature();
        Set<String> baseIRIs = new HashSet<>();

        for (OWLEntity entity : entities) {
            IRI iri = entity.getIRI();
            String iriString = iri.toString();
            System.out.println(iriString);
            // Remove the local fragment (after last '/' or '#') to find base IRI
            int lastSlash = iriString.lastIndexOf('/');
            int lastHash = iriString.lastIndexOf('#');
            int splitIndex = Math.max(lastSlash, lastHash);

            if (splitIndex > -1) {
                String currentBase = iriString.substring(0, splitIndex + 1);
                baseIRIs.add(currentBase);
            }
        }
        return Optional.of(baseIRIs);
    }


    public static void main(String[] args) throws OWLOntologyStorageException {
        File[] dir = new File("../data/ont_modules/").listFiles();
        File parsing_error = new File("output/parsing.csv");
        File outputDir = new File("output/ontologies/");

        // Create output directory if it doesn't exist
        if (!outputDir.exists()) {
            System.out.println("Creating Output Directory");
            outputDir.mkdirs();
        }

        for (File file : dir) {
            if (file.getName().equals(".gitkeep")) continue;
            OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
            OWLOntology ontology = null;
            try {
                ontology = manager.loadOntologyFromOntologyDocument(file);
            } catch (Exception e) {
                try (BufferedWriter bw = new BufferedWriter(new FileWriter(parsing_error))) {
                    bw.write(file.getName());
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
            if (ontology == null) continue;
            // Remove Annotation Axioms
            removeAnnotationAxioms(ontology, manager);
            removeSKOSAxioms(ontology, manager);


            ManchesterSyntaxDocumentFormat manchesterSyntaxDocumentFormat = new ManchesterSyntaxDocumentFormat();
            Optional<Set<String>> prefixes = findBaseIRI(ontology, manager);
            if(prefixes.isPresent()){
                System.out.println(prefixes);
                DefaultPrefixManager pm = new DefaultPrefixManager();
                for(String prefix : prefixes.get()){
                    String nameTag = prefix.replaceFirst("^(http://|https://)", "")
                            .replace("www\\.", "")
                            .replaceFirst("^(\\.org|\\.de|\\.edu)", "");

                    pm.setPrefix(nameTag, prefix);
                }
                manchesterSyntaxDocumentFormat.copyPrefixesFrom(pm);
            }

            try {
                manager.saveOntology(ontology, manchesterSyntaxDocumentFormat, IRI.create(new File(outputDir, file.getName()).toURI()));
                System.out.println("Successfully saved Ontology: " + file.getName());
            } catch (OWLOntologyStorageException e) {
                System.err.println("Failed to save ontology: " + file.getName() + " due to " + e.getMessage());
            }
        }
    }
}