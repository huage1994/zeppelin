package org.apache.zeppelin.notebook.repo;

import java.util.LinkedList;

public class RevisionsInfo extends LinkedList<NotebookRepoWithVersionControl.Revision> {

    public static RevisionsInfo fromText(String revisionsText) {
        RevisionsInfo revisionsInfo = new RevisionsInfo();
        for (String line : revisionsText.split("\\n")) {
            String[] parts = line.split(",");
            NotebookRepoWithVersionControl.Revision revision = new NotebookRepoWithVersionControl.Revision(parts[0], parts[1], Integer.parseInt(parts[2]));
            revisionsInfo.add(revision);
        }
        return revisionsInfo;
    }

    public String toText() {
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < this.size(); i++) {
            stringBuffer.append(this.get(i).id + "," + this.get(i).message + "," + this.get(i).time + "\n");
        }
        return stringBuffer.toString();
    }
}
