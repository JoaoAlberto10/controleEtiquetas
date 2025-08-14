import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.modelcore.MGEModelException;
import br.com.sankhya.modelcore.PlatformService;
import br.com.sankhya.modelcore.PlatformServiceFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

public class TRG_CONFERENCIA_TGFITE implements EventoProgramavelJava {

    @Override public void beforeInsert(PersistenceEvent event) throws Exception {
        validarConferencia((DynamicVO) event.getVo(), event);
    }
    @Override public void beforeUpdate(PersistenceEvent event) throws Exception {
        // validarConferencia((DynamicVO) event.getVo(), event);
    }
    @Override public void afterDelete(PersistenceEvent event) {}
    @Override public void beforeCommit(TransactionContext tranCtx) {}
    @Override public void afterUpdate(PersistenceEvent event) throws Exception {}
    @Override public void beforeDelete(PersistenceEvent event) {}
    @Override public void afterInsert(PersistenceEvent event)  throws Exception {
        DynamicVO vo = (DynamicVO) event.getVo();
        JdbcWrapper jdbc = event.getJdbcWrapper();
        final BigDecimal nuNota  = vo.asBigDecimal("NUNOTA");
        final BigDecimal codProd = vo.asBigDecimal("CODPROD");
        final BigDecimal seqVol  = vo.asBigDecimal("AD_SEQVOL");
        final BigDecimal codUsu  = vo.asBigDecimal("AD_CODUSUINC");
        final String     codVol  = vo.asString("CODVOL");
        final BigDecimal    codLocal   = vo.asBigDecimal("CODLOCALORIG");
        final String     codBarraItem = vo.asString("CODBARRAPDV");

        if (seqVol != null) {

            // 1) capacidade do palete (TGFVOA.AD_QTDPALETE)
            final Integer itensPorPalete = obterQtdPorPalete(jdbc, codProd, codVol);
            if (itensPorPalete == null || itensPorPalete <= 0) {
                return;
            }

            // 2) total atual de itens já gravados (sem contar o que está inserindo agora)
            final int totalJaGravados = contarItensIvc(jdbc, nuNota, codProd, codVol, seqVol);

            // 3.1) tipo do palete a partir do volume do item
            final String tipoPalete;
            if ("CX".equalsIgnoreCase(codVol)) tipoPalete = "PX";
            else if ("CN".equalsIgnoreCase(codVol)) tipoPalete = "PN";
            else tipoPalete = codVol; // fallback: mesmo volume

            // 3.2) número sequencial do palete dentro do grupo
            String seqPalete3 = String.format("%03d", seqVol.intValue());

            // 3.3) construir CODBARRA do palete: <CODPROD><tipoPalete>-<NUCONF>-<seqPalete3>
            final String codBarraPalete = codProd.toPlainString() + tipoPalete + "-" +
                    nuNota.toPlainString() + "-" + seqPalete3;

                NativeSql upd = new NativeSql(jdbc);
                upd.appendSql("UPDATE AD_FTICODBARRAITEM SET CODBARRAPALETE = :PAL WHERE CODBARRA = :ETI AND CODBARRAPALETE IS NULL");
                upd.setNamedParameter("PAL", codBarraPalete);
                upd.setNamedParameter("ETI", codBarraItem);
                upd.executeUpdate();


            // 3.7) inserir etiqueta do palete em TGFBAR (se ainda não existir)
            if (itensPorPalete == totalJaGravados) {
                NativeSql ins = new NativeSql(jdbc);
                ins.appendSql(
                        "INSERT INTO AD_FTICODBARRAPALETE (NROCONFIG, CODBARRAPALETE, CODPROD, CODVOL, DHINC, CODUSU, CODLOCAL, NUNOTA) " +
                                "VALUES (1, :CODBARRA, :CODPROD, :CODVOL, SYSDATE, :CODUSU, :LOCAL, :NUNOTA)"
                );
                ins.setNamedParameter("CODBARRA", codBarraPalete);
                ins.setNamedParameter("CODPROD", codProd);
                ins.setNamedParameter("CODVOL", tipoPalete);      // PX/PN (ou fallback)
                ins.setNamedParameter("CODUSU", codUsu);
                ins.setNamedParameter("LOCAL", codLocal);
                ins.setNamedParameter("NUNOTA", nuNota);
                ins.executeUpdate();

                String localPrinterName = "168.90.183.146:9091/ZD230CPO02001";
                PlatformService reportService = PlatformServiceFactory.getInstance().lookupService("@core:report.service");
                reportService.set("printer.name", localPrinterName);
                reportService.set("nurfe", 287);
                reportService.set("codemp", BigDecimal.ONE);

                Map<String, Object> parameters = new HashMap<>();
                parameters.put("P_CODBARRA", codBarraPalete);
                reportService.set("report.params", parameters);
                System.out.println("Executando o relatório - ASM_SNK");
                reportService.execute();
            }
        }
    }

    private void validarConferencia(DynamicVO notaVO, PersistenceEvent event) throws Exception {
        BigDecimal nuNota = notaVO.asBigDecimal("NUNOTA");
        String codBarra   = notaVO.asString("CODBARRAPDV");
        String codVolNovo = notaVO.asString("CODVOL");
        BigDecimal codProd= notaVO.asBigDecimal("CODPROD");
        BigDecimal qtdconf= new BigDecimal(1);

        BigDecimal top = null;
        BigDecimal codVolPedido = null;
        BigDecimal codBarraConferido = null;
        BigDecimal qtdPendente = null;
        BigDecimal nuNotaOrig = null;

        // ========== 1) Consulta TOP / NUNOTAORIG (AJUSTE: espaços e binds) ==========
        NativeSql sql = new NativeSql(event.getJdbcWrapper());
        sql.appendSql(
                "SELECT CAB2.CODTIPOPER, CON.NUNOTAORIG " +
                        "  FROM AD_FTICONFERENCIA CON " +
                        "  INNER JOIN TGFCAB CAB  ON CAB.NUNOTA  = CON.NUNOTAORIG " +
                        "  LEFT  JOIN TGFCAB CAB2 ON CAB2.NUNOTA = CON.NUNOTACONF " +
                        " WHERE CAB2.NUNOTA = :NUNOTA "
        );
        sql.setNamedParameter("NUNOTA", nuNota);
        try (ResultSet rs = sql.executeQuery()) {
            if (rs.next()) {
                top = rs.getBigDecimal("CODTIPOPER");
                nuNotaOrig = rs.getBigDecimal("NUNOTAORIG");
            }
        }

        if (top != null && (top.intValue() == 1700 || top.intValue() == 1701)) {

            // ========== 2) 3 subconsultas (AJUSTE: tirar hardcode e corrigir alias) ==========
            NativeSql sql2 = new NativeSql(event.getJdbcWrapper());
            sql2.appendSql(
                    "SELECT " +
                            "  (SELECT COUNT(*) " +
                            "     FROM TGFITE " +
                            "    WHERE NUNOTA = :NUNOTACONF AND CODBARRAPDV = :CODBARRA) AS CODBARRAREPETIDO, " +

                            "  (SELECT COUNT(*) " +
                            "     FROM TGFITE ITE " +
                            "    WHERE ITE.NUNOTA = :NUNOTAORIG " +
                            "      AND ITE.CODPROD = :CODPROD " +
                            "      AND ITE.CODVOL  = :CODVOL) AS CODVOLPEDIDO, " +

                            "  NVL( " +
                            "     ( NVL((SELECT SUM(CASE WHEN ITE.CODVOL = PRO.CODVOL " +
                            "                             THEN ITE.QTDNEG " +
                            "                             ELSE ITE.QTDNEG / NVL(VOA.QUANTIDADE,1) END) " +
                            "               FROM TGFITE ITE " +
                            "               JOIN TGFPRO PRO ON PRO.CODPROD = ITE.CODPROD " +
                            "               LEFT JOIN TGFVOA VOA ON VOA.CODPROD = ITE.CODPROD AND VOA.CODVOL = ITE.CODVOL " +
                            "              WHERE ITE.NUNOTA = :NUNOTAORIG " +
                            "                AND ITE.CODPROD = :CODPROD " +
                            "                AND ITE.CODVOL  = :CODVOL), 0) " +
                            "       - " +
                            "       NVL((SELECT SUM(CASE WHEN ITE2.CODVOL = PRO2.CODVOL " +
                            "                           THEN ITE2.QTDNEG " +
                            "                           ELSE ITE2.QTDNEG / NVL(VOA2.QUANTIDADE,1) END) " +
                            "               FROM TGFCAB CAB2 " +
                            "               JOIN TGFITE ITE2 ON ITE2.NUNOTA = CAB2.NUNOTA " +
                            "               JOIN TGFPRO PRO2 ON PRO2.CODPROD = ITE2.CODPROD " +
                            "               LEFT JOIN TGFVOA VOA2 ON VOA2.CODPROD = ITE2.CODPROD AND VOA2.CODVOL = ITE2.CODVOL " +
                            "              WHERE CAB2.NUNOTA = :NUNOTACONF " +
                            "                AND ITE2.CODPROD = :CODPROD " +
                            "                AND ITE2.CODVOL  = :CODVOL), 0) " +
                            "     ), 0) AS QTDPENDENTE " +
                            "FROM DUAL "
            );
            sql2.setNamedParameter("NUNOTACONF", nuNota);
            sql2.setNamedParameter("CODBARRA", codBarra);
            sql2.setNamedParameter("NUNOTAORIG", nuNotaOrig);
            sql2.setNamedParameter("CODPROD", codProd);
            sql2.setNamedParameter("CODVOL", codVolNovo);

            try (ResultSet rs2 = sql2.executeQuery()) {
                if (rs2.next()) {
                    codBarraConferido = rs2.getBigDecimal("CODBARRAREPETIDO");
                    codVolPedido = rs2.getBigDecimal("CODVOLPEDIDO");
                    qtdPendente = rs2.getBigDecimal("QTDPENDENTE");
                }
            }

            if (qtdPendente == null) qtdPendente = BigDecimal.ZERO;

            // ========== 3) Mesmas validações que você já tinha ==========
            if (qtdconf != null && qtdconf.compareTo(qtdPendente) > 0) {
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

    private Integer obterQtdPorPalete(JdbcWrapper jdbc, BigDecimal codProd, String codVol) throws Exception {
        NativeSql ns = new NativeSql(jdbc);
        ns.appendSql("SELECT VOA.AD_QTDPALETE FROM TGFVOA VOA WHERE VOA.CODPROD = :P1 AND VOA.CODVOL = :P2");
        ns.setNamedParameter("P1", codProd);
        ns.setNamedParameter("P2", codVol);
        ResultSet rs = ns.executeQuery();
        Integer q = null;
        if (rs.next()) {
            BigDecimal bd = rs.getBigDecimal(1);
            if (bd != null) q = bd.intValue();
        }
        rs.close();
        return q;
    }

    private int contarItensIvc(JdbcWrapper jdbc, BigDecimal nuNota, BigDecimal codProd, String codVol, BigDecimal seqVol) throws Exception {
        NativeSql ns = new NativeSql(jdbc);
        ns.appendSql("SELECT COUNT(*) FROM TGFITE WHERE NUNOTA = :C1 AND CODPROD = :C2 AND CODVOL = :C3 AND AD_SEQVOL = :C4");
        ns.setNamedParameter("C1", nuNota);
        ns.setNamedParameter("C2", codProd);
        ns.setNamedParameter("C3", codVol);
        ns.setNamedParameter("C4", seqVol);
        ResultSet rs = ns.executeQuery();
        int c = 0;
        if (rs.next()) c = rs.getInt(1);
        rs.close();
        return c;
    }
}
