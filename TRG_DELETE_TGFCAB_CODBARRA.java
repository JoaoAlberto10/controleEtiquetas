import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;


public class TRG_DELETE_TGFCAB_CODBARRA implements EventoProgramavelJava {
    @Override public void afterDelete(PersistenceEvent event) {
        DynamicVO notaVO = (DynamicVO) event.getVo();
        int nuNota = notaVO.asInt("NUNOTA");
        int codTipOper = notaVO.asInt("CODTIPOPER");

        if (codTipOper == 406 || codTipOper == 407) {
            JdbcWrapper jdbc = event.getJdbcWrapper();
            apagarBARCODE(jdbc, nuNota);
        }
    }

    private void apagarBARCODE(JdbcWrapper jdbc, int nuNota) {
        try {
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("DELETE FROM AD_FTICODBARRAITEM WHERE NUNOTA = :NUNOTA");
            sql.setNamedParameter("NUNOTA", nuNota);
            sql.executeUpdate();

            NativeSql sql2 = new NativeSql(jdbc);
            sql2.appendSql("DELETE FROM AD_FTICODBARRAPALETE WHERE NUNOTA = :NUNOTA");
            sql2.setNamedParameter("NUNOTA", nuNota);
            sql2.executeUpdate();

            NativeSql sql3 = new NativeSql(jdbc);
            sql3.appendSql("DELETE FROM AD_FTICONFERENCIA WHERE NUNOTAORIG = :NUNOTA");
            sql3.setNamedParameter("NUNOTA", nuNota);
            sql3.executeUpdate();
            System.out.println("Etiquetas da nota " + nuNota + " removidas.");
        } catch (Exception e) {
            System.err.println("Erro ao deletar etiquetas da nota " + nuNota + ": " + e.getMessage());
        }
    }
    @Override public void afterUpdate(PersistenceEvent event) throws Exception {}
    @Override public void beforeInsert(PersistenceEvent event) {}
    @Override public void beforeUpdate(PersistenceEvent event) {}
    @Override public void beforeDelete(PersistenceEvent event) {}
    @Override public void afterInsert(PersistenceEvent event) {}
    @Override public void beforeCommit(TransactionContext tranCtx) {}
}
