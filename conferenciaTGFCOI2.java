import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.modelcore.MGEModelException;

import java.math.BigDecimal;
import java.sql.ResultSet;

public class conferenciaTGFCOI2 implements EventoProgramavelJava {

    @Override public void beforeInsert(PersistenceEvent event) throws Exception {
        validarConferencia((DynamicVO) event.getVo(), event);
    }
    @Override public void beforeUpdate(PersistenceEvent event) throws Exception {
        validarConferencia((DynamicVO) event.getVo(), event);
    }
    @Override public void afterUpdate(PersistenceEvent event) throws Exception {}
    @Override public void beforeDelete(PersistenceEvent event) {}
    @Override public void afterInsert(PersistenceEvent event) {}
    @Override public void afterDelete(PersistenceEvent event) {}
    @Override public void beforeCommit(TransactionContext tranCtx) {}

    private void validarConferencia(DynamicVO notaVO, PersistenceEvent event) throws Exception {
        BigDecimal nuConf = notaVO.asBigDecimal("NUCONF");
        String codBarra = notaVO.asString("CODBARRA");
        String codVolNovo = notaVO.asString("CODVOL");
        BigDecimal codProd = notaVO.asBigDecimal("CODPROD");
        BigDecimal qtdconf = notaVO.asBigDecimal("QTDCONF");

        BigDecimal top = null;
        BigDecimal codVolPedido = null;
        BigDecimal codBarraConferido = null;
        BigDecimal qtdPendente = null;

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
                            "WHERE CON.NUCONF = :NUCONF AND ITE.CODPROD = :CODPROD AND ITE.CODVOL = :CODVOL) AS CODVOLPEDIDO, " +
                            "NVL((SELECT SUM(CASE WHEN ITE.CODVOL=PRO.CODVOL THEN ITE.QTDNEG ELSE " +
                            "ITE.QTDNEG/VOA.QUANTIDADE END) AS QTDITENS FROM TGFCON2 CON " +
                            "JOIN TGFITE ITE ON ITE.NUNOTA = CON.NUNOTAORIG " +
                            "INNER JOIN TGFPRO PRO ON PRO.CODPROD=ITE.CODPROD " +
                            "LEFT JOIN TGFVOA VOA ON VOA.CODPROD=ITE.CODPROD AND VOA.CODVOL=ITE.CODVOL " +
                            "WHERE CON.NUCONF = :NUCONF AND ITE.CODPROD = :CODPROD AND ITE.CODVOL = :CODVOL),0) - " +
                            "NVL((SELECT SUM(QTDCONF) FROM TGFCOI2 WHERE NUCONF = :NUCONF AND CODPROD= :CODPROD AND CODVOL = :CODVOL),0) AS QTDPENDENTE " +
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
                qtdPendente = rs2.getBigDecimal("QTDPENDENTE");
            }
            rs2.close();

            if (qtdconf.compareTo(qtdPendente) > 0) {
                throw new MGEModelException("Quantidade acima do pedido original!");
            }

            if (codVolPedido == null || codVolPedido.compareTo(BigDecimal.ZERO) == 0) {
                throw new MGEModelException("Volume informado não faz parte do pedido original!");
            }

            if (codBarraConferido != null && codBarraConferido.compareTo(BigDecimal.ZERO) > 0) {
                throw new MGEModelException("Este código de barras já foi conferido anteriormente!");
            }
        }
    }
}
