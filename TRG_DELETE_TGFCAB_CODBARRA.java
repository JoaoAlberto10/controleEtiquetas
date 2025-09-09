import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import java.sql.ResultSet;


public class TRG_DELETE_TGFCAB_CODBARRA implements EventoProgramavelJava {
    @Override public void beforeDelete(PersistenceEvent event) throws Exception{DynamicVO notaVO = (DynamicVO) event.getVo();
        int nuNota = notaVO.asInt("NUNOTA");
        int codTipOper = notaVO.asInt("CODTIPOPER");

        JdbcWrapper jdbc = event.getJdbcWrapper();

        NativeSql sqlCheck = new NativeSql(jdbc);
        sqlCheck.appendSql(
                "SELECT 1 FROM AD_FTICONFERENCIA " +
                        "WHERE NUNOTAORIG = :NUNOTA " +
                        "AND NUNOTACONF IS NOT NULL"
        );
        sqlCheck.setNamedParameter("NUNOTA", nuNota);

        try (ResultSet rs = sqlCheck.executeQuery()) {
            if (rs.next()) {
                throw new Exception("Não é permitido excluir a nota " + nuNota +
                        " pois existe conferência vinculada.");
            }
        }

        if (codTipOper == 1700 || codTipOper == 1701 || codTipOper == 1702 || codTipOper == 1703) {
            apagarCONFERENCIA(jdbc, nuNota);
        } else {
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
            sql2.appendSql("DELETE FROM AD_FTICONFERENCIA WHERE NUNOTAORIG = :NUNOTA");
            sql2.setNamedParameter("NUNOTA", nuNota);
            sql2.executeUpdate();
        } catch (Exception e) {
            System.err.println("Erro ao deletar etiquetas da nota " + nuNota + ": " + e.getMessage());
        }
    }

    private void apagarCONFERENCIA(JdbcWrapper jdbc, int nuNota) {
        try {
            NativeSql sql = new NativeSql(jdbc);
            sql.appendSql("UPDATE AD_FTICONFERENCIA SET NUNOTACONF = NULL WHERE NUNOTACONF = :NUNOTA");
            sql.setNamedParameter("NUNOTA", nuNota);
            sql.executeUpdate();

            NativeSql sql2 = new NativeSql(jdbc);
            sql2.appendSql("DELETE FROM AD_FTICODBARRAPALETE WHERE NUNOTA = :NUNOTA");
            sql2.setNamedParameter("NUNOTA", nuNota);
            sql2.executeUpdate();

            NativeSql sql3 = new NativeSql(jdbc);
            sql3.appendSql("UPDATE AD_FTICONFERENCIA SET NUNOTACONF = NULL WHERE NUNOTACONF = :NUNOTA");
            sql3.setNamedParameter("NUNOTA", nuNota);
            sql3.executeUpdate();
        } catch (Exception e) {
            System.err.println("Erro ao deletar etiquetas da nota " + nuNota + ": " + e.getMessage());
        }
    }
    @Override public void afterDelete(PersistenceEvent event) {}
    @Override public void afterUpdate(PersistenceEvent event) {}
    @Override public void beforeInsert(PersistenceEvent event) {}
    @Override public void beforeUpdate(PersistenceEvent event) {}
    @Override public void afterInsert(PersistenceEvent event) {}
    @Override public void beforeCommit(TransactionContext tranCtx) {}
}
