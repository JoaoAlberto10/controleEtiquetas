import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;


public class etiquetasTGFBAR implements EventoProgramavelJava {
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
            sql.appendSql("DELETE FROM TGFBAR WHERE AD_NUNOTA = :NUNOTA");
            sql.setNamedParameter("NUNOTA", nuNota);
            sql.executeUpdate();
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
