package org.dice_group.lod.down;

import org.apache.jena.rdf.model.RDFNode;

public class DatasetMetaData {

    public final String datadoc;
    public final String md5;
    public final String url;
    public final String triples;
    public final String parent;

    public DatasetMetaData(String datadoc, String md5, String url, String triples, String parent) {
        this.datadoc = datadoc;
        this.md5 = md5;
        this.url = url;
        this.triples = triples;
        if ((parent != null) && (!parent.isEmpty())) {
            this.parent = parent;
        } else {
            this.parent = null;
        }
    }

    public DatasetMetaData(RDFNode datadocNode, RDFNode md5Node, RDFNode urlNode, RDFNode triplesNode,
            RDFNode parentNode) {
        this.datadoc = datadocNode.toString();
        this.md5 = md5Node.toString();
        this.url = urlNode.toString();
        this.triples = triplesNode.toString();
        if ((parentNode != null) && (!parentNode.toString().isEmpty())) {
            this.parent = parentNode.toString();
        } else {
            this.parent = null;
        }
    }

}
