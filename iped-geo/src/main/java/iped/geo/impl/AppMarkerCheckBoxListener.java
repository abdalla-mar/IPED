package iped.geo.impl;

import javax.swing.JTable;

import iped.data.IItemId;
import iped.geo.MarkerCheckBoxListener;
import iped.geo.kml.GetResultsKMLWorker;
import iped.search.IMultiSearchResult;

public class AppMarkerCheckBoxListener implements MarkerCheckBoxListener {
    AppMapPanel mapaPanel;

    public AppMarkerCheckBoxListener(AppMapPanel mapaPanel) {
        this.mapaPanel = mapaPanel;
    }

    @Override
    public void onClicked(String mid, boolean checked) {
        // desabilita renderização automatica pela alteração no modelo por ser uma
        // alteração feita no próprio mapa;
        MapViewer.desabilitaTemp = true;

        // procura pela posição correspondente na tabela do item clicado no mapa
        int pos = 0;
        mid = GetResultsKMLWorker.getBaseGID(mid);
        IMultiSearchResult results = mapaPanel.getResultsProvider().getResults();
        for (int i = 0; i < results.getLength(); i++) {
            IItemId item = results.getItem(i);
            String gid = "marker_" + item.getSourceId() + "_" + item.getId(); //$NON-NLS-1$ //$NON-NLS-2$
            if (mid.equals(gid)) {
                pos = i;
                break;
            }
        }

        JTable t = mapaPanel.getResultsProvider().getResultsTable();
        t.setValueAt(checked, t.convertRowIndexToView(pos), t.convertColumnIndexToView(1));
    }

}
