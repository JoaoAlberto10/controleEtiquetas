import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.modelcore.MGEModelException;

import java.math.BigDecimal;
import java.sql.ResultSet;

public class conferenciaTGFCOI2 implements EventoProgramavelJava {

    @Override
    public void beforeInsert(PersistenceEvent event) throws Exception {
        DynamicVO notaVO = (DynamicVO) event.getVo();
        BigDecimal nuConf = notaVO.asBigDecimal("NUCONF");
        String codBarra = notaVO.asString("CODBARRA");
        String codVolNovo = notaVO.asString("CODVOL");
        BigDecimal codProd = notaVO.asBigDecimal("CODPROD");

        BigDecimal top = null;
        BigDecimal codVolPedido = null;
        BigDecimal codBarraConferido = null;

        // Consulta TOP
        NativeSql sql = new NativeSql(event.getJdbcWrapper());
        sql.appendSql(
                "SELECT DISTINCT CAB.CODTIPOPER AS CODTIPOPER " +
                        "FROM TGFCON2 CON " +
                        "JOIN TGFCAB CAB ON CAB.NUNOTA = CON.NUNOTAORIG " +
                        "WHERE CON.NUCONF = :NUCONF"
        );
        sql.setNamedParameter("NUCONF", nuConf);
        ResultSet rs = sql.executeQuery();
        if (rs.next()) {
            top = rs.getBigDecimal("CODTIPOPER");
        }
        rs.close();

        if (top != null && (top.intValue() == 406 || top.intValue() == 407)) {
            NativeSql sql2 = new NativeSql(event.getJdbcWrapper());
            sql2.appendSql(
                    "SELECT " +
                            "(SELECT COUNT(*) FROM TGFCOI2 WHERE NUCONF = :NUCONF AND CODBARRA = :CODBARRA) AS CODBARRAREPETIDO, " +
                            "(SELECT COUNT(*) FROM TGFCON2 CON " +
                            "JOIN TGFITE ITE ON ITE.NUNOTA = CON.NUNOTAORIG " +
                            "WHERE CON.NUCONF = :NUCONF AND ITE.CODPROD = :CODPROD AND ITE.CODVOL = :CODVOL) AS CODVOLPEDIDO " +
                            "FROM DUAL"
            );
            sql2.setNamedParameter("NUCONF", nuConf);
            sql2.setNamedParameter("CODPROD", codProd);
            sql2.setNamedParameter("CODVOL", codVolNovo);
            sql2.setNamedParameter("CODBARRA", codBarra);
            ResultSet rs2 = sql2.executeQuery();

            if (rs2.next()) {
                codBarraConferido = rs2.getBigDecimal("CODBARRAREPETIDO");
                codVolPedido = rs2.getBigDecimal("CODVOLPEDIDO");
            }
            rs2.close();

            if (codVolPedido == null || codVolPedido.compareTo(BigDecimal.ZERO) == 0) {
                throw new MGEModelException("VOLUME INFORMADO NÃO FAZ PARTE DO PEDIDO ORIGINAL!");
            }

            if (codBarraConferido != null && codBarraConferido.compareTo(BigDecimal.ZERO) > 0) {
                throw new MGEModelException("PRODUTO JÁ FOI CONTABILIZADO!");
            }
        }
    }

    @Override public void beforeUpdate(PersistenceEvent event) throws Exception {
        DynamicVO notaVO = (DynamicVO) event.getVo();
        BigDecimal nuConf = notaVO.asBigDecimal("NUCONF");
        String codBarra = notaVO.asString("CODBARRA");
        String codVolNovo = notaVO.asString("CODVOL");
        BigDecimal codProd = notaVO.asBigDecimal("CODPROD");

        BigDecimal top = null;
        BigDecimal codVolPedido = null;
        BigDecimal codBarraConferido = null;

        // Consulta TOP
        NativeSql sql = new NativeSql(event.getJdbcWrapper());
        sql.appendSql(
                "SELECT DISTINCT CAB.CODTIPOPER AS CODTIPOPER " +
                        "FROM TGFCON2 CON " +
                        "JOIN TGFCAB CAB ON CAB.NUNOTA = CON.NUNOTAORIG " +
                        "WHERE CON.NUCONF = :NUCONF"
        );
        sql.setNamedParameter("NUCONF", nuConf);
        ResultSet rs = sql.executeQuery();
        if (rs.next()) {
            top = rs.getBigDecimal("CODTIPOPER");
        }
        rs.close();

        if (top != null && (top.intValue() == 406 || top.intValue() == 407)) {
            NativeSql sql2 = new NativeSql(event.getJdbcWrapper());
            sql2.appendSql(
                    "SELECT " +
                            "(SELECT COUNT(*) FROM TGFCOI2 WHERE NUCONF = :NUCONF AND CODBARRA = :CODBARRA) AS CODBARRAREPETIDO, " +
                            "(SELECT COUNT(*) FROM TGFCON2 CON " +
                            "JOIN TGFITE ITE ON ITE.NUNOTA = CON.NUNOTAORIG " +
                            "WHERE CON.NUCONF = :NUCONF AND ITE.CODPROD = :CODPROD AND ITE.CODVOL = :CODVOL) AS CODVOLPEDIDO " +
                            "FROM DUAL"
            );
            sql2.setNamedParameter("NUCONF", nuConf);
            sql2.setNamedParameter("CODPROD", codProd);
            sql2.setNamedParameter("CODVOL", codVolNovo);
            sql2.setNamedParameter("CODBARRA", codBarra);
            ResultSet rs2 = sql2.executeQuery();

            if (rs2.next()) {
                codBarraConferido = rs2.getBigDecimal("CODBARRAREPETIDO");
                codVolPedido = rs2.getBigDecimal("CODVOLPEDIDO");
            }
            rs2.close();

            if (codVolPedido == null || codVolPedido.compareTo(BigDecimal.ZERO) == 0) {
                throw new MGEModelException("VOLUME INFORMADO NÃO FAZ PARTE DO PEDIDO ORIGINAL!");
            }

            if (codBarraConferido != null && codBarraConferido.compareTo(BigDecimal.ZERO) > 0) {
                throw new MGEModelException("PRODUTO JÁ FOI CONTABILIZADO!");
            }
        }
    }

    @Override public void afterUpdate(PersistenceEvent event) throws Exception {}
    @Override public void beforeDelete(PersistenceEvent event) {}
    @Override public void afterInsert(PersistenceEvent event) {}
    @Override public void afterDelete(PersistenceEvent event) {}
    @Override public void beforeCommit(TransactionContext tranCtx) {}
}
