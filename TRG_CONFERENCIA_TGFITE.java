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

    @Override
    public void beforeInsert(PersistenceEvent event) throws Exception {
        DynamicVO vo = (DynamicVO) event.getVo();
        JdbcWrapper jdbc = event.getJdbcWrapper();

        BigDecimal top = obterTopDaNota(jdbc, vo.asBigDecimal("NUNOTA"));

        if (top != null && (top.intValue() == 1700 || top.intValue() == 1701 || top.intValue() == 1702 || top.intValue() == 1703)) {
            validarConferencia(vo, event, top);
            gerarEtiquetaPalete(vo, jdbc, top);
        }else if (top != null && top.intValue() == 1704) {
            this.validarConferencia(vo, event, top); // só valida, sem gerar etiqueta
        }
    }

    @Override public void beforeUpdate(PersistenceEvent event) throws Exception { }
    @Override public void afterDelete(PersistenceEvent event) { }
    @Override public void beforeCommit(TransactionContext tranCtx) { }
    @Override public void afterUpdate(PersistenceEvent event) throws Exception { }
    @Override public void beforeDelete(PersistenceEvent event) { }
    @Override public void afterInsert(PersistenceEvent event) throws Exception { }

    private void validarConferencia(DynamicVO notaVO, PersistenceEvent event, BigDecimal top) throws Exception {
        JdbcWrapper jdbc = event.getJdbcWrapper();
        BigDecimal nuNota = notaVO.asBigDecimal("NUNOTA");
        String codBarra = notaVO.asString("CODBARRAPDV");
        String codVolNovo = notaVO.asString("CODVOL");
        BigDecimal codProd = notaVO.asBigDecimal("CODPROD");
        BigDecimal qtdconf = BigDecimal.ONE;
        BigDecimal codLocalItem = notaVO.asBigDecimal("CODLOCALORIG");

        // --- 🔒 Validação: não permitir bipar caixa e palete do mesmo conjunto ---
        NativeSql sqlVerifica = new NativeSql(jdbc);
        sqlVerifica.appendSql(
                "SELECT 'X' " +
                        "FROM AD_FTICODBARRAITEM ETI " +
                        "WHERE (" +
                        "   (ETI.CODBARRA = :CODBARRA AND EXISTS ( " +
                        "       SELECT 1 FROM TGFITE ITE2 WHERE ITE2.NUNOTA = :NUNOTA AND ITE2.CODBARRAPDV = ETI.CODBARRAPALETE " +
                        "   )) " +
                        "   OR " +
                        "   (ETI.CODBARRAPALETE = :CODBARRA AND EXISTS ( " +
                        "       SELECT 1 FROM TGFITE ITE3 WHERE ITE3.NUNOTA = :NUNOTA AND ITE3.CODBARRAPDV = ETI.CODBARRA " +
                        "   ))" +
                        ")"
        );
        sqlVerifica.setNamedParameter("CODBARRA", codBarra);
        sqlVerifica.setNamedParameter("NUNOTA", nuNota);

        try (ResultSet rs = sqlVerifica.executeQuery()) {
            if (rs.next()) {
                throw new MGEModelException("Não é permitido bipar o palete e suas caixas na mesma nota!");
            }
        }

        BigDecimal codVolPedido = null;
        BigDecimal codBarraConferido = null;
        BigDecimal qtdPendente = null;
        BigDecimal nuNotaOrig = null;


        if (top.intValue() == 1704) {
            // Inventário não movimenta estoque, apenas valida duplicidade
            NativeSql sqlDup = new NativeSql(jdbc);
            sqlDup.appendSql("SELECT COUNT(*) QT FROM TGFITE WHERE NUNOTA = :NUNOTA AND CODBARRAPDV = :CODBARRA");
            sqlDup.setNamedParameter("NUNOTA", nuNota);
            sqlDup.setNamedParameter("CODBARRA", codBarra);

            try (ResultSet rs = sqlDup.executeQuery()) {
                if (rs.next()) {
                    BigDecimal qtd = rs.getBigDecimal("QT");
                    if (qtd != null && qtd.compareTo(BigDecimal.ZERO) > 0) {
                        throw new MGEModelException("Este código de barras já foi informado nesta nota de inventário!");
                    }
                }
            }
            return;
        }

        // Busca NUNOTAORIG
        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql(
                "SELECT CON.NUNOTAORIG " +
                        "FROM AD_FTICONFERENCIA CON " +
                        "INNER JOIN TGFCAB CAB2 ON CAB2.NUNOTA = CON.NUNOTACONF " +
                        "WHERE CAB2.NUNOTA = :NUNOTA"
        );
        sql.setNamedParameter("NUNOTA", nuNota);
        try (ResultSet rs = sql.executeQuery()) {
            if (rs.next()) nuNotaOrig = rs.getBigDecimal("NUNOTAORIG");
        }

        // Validação de LOCAL conforme TOP
        BigDecimal codLocalEtiqueta = null;
        NativeSql sqlLoc = new NativeSql(jdbc);
        sqlLoc.appendSql("SELECT CODLOCAL FROM AD_FTICODBARRAITEM WHERE CODBARRA = :CODB AND ATIVO='S' UNION ALL SELECT CODLOCAL FROM AD_FTICODBARRAPALETE WHERE CODBARRAPALETE = :CODB AND ATIVO='S'");
        sqlLoc.setNamedParameter("CODB", codBarra);
        try (ResultSet rs = sqlLoc.executeQuery()) {
            if (rs.next()) codLocalEtiqueta = rs.getBigDecimal("CODLOCAL");
            else throw new MGEModelException("Código de barras inválido ou inativo.");
        }

        if (top.intValue() == 1701) { // Saída
            if (codLocalEtiqueta == null || codLocalItem == null || !codLocalEtiqueta.equals(codLocalItem)) {
                throw new MGEModelException("Local da etiqueta não corresponde ao local informado na nota (Saída).");
            }
        } else if (top.intValue() == 1702) { // Entrada
            if (codLocalEtiqueta != null) {
                throw new MGEModelException("Etiqueta já possui local preenchido, deveria estar NULL (Entrada).");
            }
        } else if (top.intValue() == 1703) { // Transferência
            BigDecimal codLocalConf = null;
            NativeSql sqlConf = new NativeSql(jdbc);
            sqlConf.appendSql("SELECT CODLOCAL FROM AD_FTICONFERENCIA WHERE NUNOTACONF = :NUNOTA");
            sqlConf.setNamedParameter("NUNOTA", nuNota);
            try (ResultSet rs = sqlConf.executeQuery()) {
                if (rs.next()) codLocalConf = rs.getBigDecimal("CODLOCAL");
            }
            if (codLocalEtiqueta == null || codLocalConf == null || !codLocalEtiqueta.equals(codLocalConf)) {
                throw new MGEModelException("Local da etiqueta não corresponde ao local da conferência (Transferência).");
            }
        }

        // Validações gerais de quantidade e volume
        NativeSql sql2 = new NativeSql(jdbc);
        sql2.appendSql(
                "SELECT " +
                        " (SELECT COUNT(*) FROM TGFITE WHERE NUNOTA = :NUNOTACONF AND CODBARRAPDV = :CODBARRA) AS CODBARRAREPETIDO, " +
                        " (SELECT COUNT(*) FROM TGFITE ITE WHERE ITE.NUNOTA = :NUNOTAORIG AND ITE.CODPROD = :CODPROD AND (ITE.CODVOL = :CODVOL OR ITE.AD_CODVOL = :CODVOL)) AS CODVOLPEDIDO, " +
                        " NVL(( NVL((SELECT SUM(CASE " +
                        " WHEN ITE.AD_CODVOL IS NULL AND ITE.CODVOL = PRO.CODVOL THEN ITE.QTDNEG " +
                        " WHEN ITE.AD_CODVOL IS NULL AND ITE.CODVOL <> PRO.CODVOL THEN ITE.QTDNEG / NVL(VOA.QUANTIDADE,1) " +
                        " WHEN ITE.AD_CODVOL IS NOT NULL AND ITE.AD_CODVOL = PRO.CODVOL THEN ITE.QTDNEG " +
                        " WHEN ITE.AD_CODVOL IS NOT NULL AND ITE.AD_CODVOL <> PRO.CODVOL THEN ITE.QTDNEG / NVL(VOA2.QUANTIDADE,1) END) " +
                        " FROM TGFITE ITE JOIN TGFPRO PRO ON PRO.CODPROD = ITE.CODPROD " +
                        " LEFT JOIN TGFVOA VOA ON VOA.CODPROD = ITE.CODPROD AND VOA.CODVOL = ITE.CODVOL " +
                        " LEFT JOIN TGFVOA VOA2 ON VOA2.CODPROD = ITE.CODPROD AND VOA2.CODVOL = ITE.AD_CODVOL " +
                        " WHERE ITE.NUNOTA = :NUNOTAORIG AND ITE.CODPROD = :CODPROD AND (ITE.CODVOL = :CODVOL OR ITE.AD_CODVOL = :CODVOL)),0) " +
                        " - NVL((SELECT SUM(CASE " +
                        " WHEN ITE2.AD_CODVOL IS NULL AND ITE2.CODVOL = PRO2.CODVOL THEN ITE2.QTDNEG " +
                        " WHEN ITE2.AD_CODVOL IS NULL AND ITE2.CODVOL <> PRO2.CODVOL THEN ITE2.QTDNEG / NVL(VOA3.QUANTIDADE,1) " +
                        " WHEN ITE2.AD_CODVOL IS NOT NULL AND ITE2.AD_CODVOL = PRO2.CODVOL THEN ITE2.QTDNEG " +
                        " WHEN ITE2.AD_CODVOL IS NOT NULL AND ITE2.AD_CODVOL <> PRO2.CODVOL THEN ITE2.QTDNEG / NVL(VOA4.QUANTIDADE,1) END) " +
                        " FROM TGFCAB CAB2 JOIN TGFITE ITE2 ON ITE2.NUNOTA = CAB2.NUNOTA " +
                        " JOIN TGFPRO PRO2 ON PRO2.CODPROD = ITE2.CODPROD " +
                        " LEFT JOIN TGFVOA VOA3 ON VOA3.CODPROD = ITE2.CODPROD AND VOA3.CODVOL = ITE2.CODVOL " +
                        " LEFT JOIN TGFVOA VOA4 ON VOA4.CODPROD = ITE2.CODPROD AND VOA4.CODVOL = ITE2.AD_CODVOL " +
                        " WHERE CAB2.NUNOTA = :NUNOTACONF AND ITE2.CODBARRAPDV IS NOT NULL AND ITE2.CODPROD = :CODPROD AND (ITE2.CODVOL = :CODVOL OR ITE2.AD_CODVOL = :CODVOL)),0)),0) AS QTDPENDENTE " +
                        "FROM DUAL"
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

    private Integer obterQtdPorPalete(JdbcWrapper jdbc, BigDecimal codProd, String codVol) throws Exception {
        NativeSql ns = new NativeSql(jdbc);
        ns.appendSql("SELECT AD_QTDPALETE FROM TGFVOA WHERE CODPROD = :P1 AND CODVOL = :P2");
        ns.setNamedParameter("P1", codProd);
        ns.setNamedParameter("P2", codVol);
        try (ResultSet rs = ns.executeQuery()) {
            if (rs.next()) {
                BigDecimal bd = rs.getBigDecimal(1);
                if (bd != null) return bd.intValue();
            }
        }
        return null;
    }

    private int contarItensIvc(JdbcWrapper jdbc, BigDecimal nuNota, BigDecimal codProd, String codVol, BigDecimal seqVol) throws Exception {
        NativeSql ns = new NativeSql(jdbc);
        ns.appendSql("SELECT COUNT(*) FROM TGFITE WHERE NUNOTA = :C1 AND CODPROD = :C2 AND CODVOL = :C3 AND AD_SEQVOL = :C4");
        ns.setNamedParameter("C1", nuNota);
        ns.setNamedParameter("C2", codProd);
        ns.setNamedParameter("C3", codVol);
        ns.setNamedParameter("C4", seqVol);
        try (ResultSet rs = ns.executeQuery()) {
            if (rs.next()) return rs.getInt(1);
        }
        return 0;
    }

    private BigDecimal obterTopDaNota(JdbcWrapper jdbc, BigDecimal nuNota) throws Exception {
        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql("SELECT CODTIPOPER FROM TGFCAB WHERE NUNOTA = :NUNOTA");
        sql.setNamedParameter("NUNOTA", nuNota);
        try (ResultSet rs = sql.executeQuery()) {
            if (rs.next()) return rs.getBigDecimal("CODTIPOPER");
        }
        return null;
    }

    private void gerarEtiquetaPalete(DynamicVO vo, JdbcWrapper jdbc, BigDecimal top) throws Exception {
        final BigDecimal nuNota = vo.asBigDecimal("NUNOTA");
        final BigDecimal codProd = vo.asBigDecimal("CODPROD");
        final BigDecimal seqVol = vo.asBigDecimal("AD_SEQVOL");
        final BigDecimal codUsu = vo.asBigDecimal("AD_CODUSUINC");
        final String codVol = vo.asString("CODVOL");
        final BigDecimal codLocalOrig = vo.asBigDecimal("CODLOCALORIG");
        final String codBarraItem = vo.asString("CODBARRAPDV");

        // Determina local
        BigDecimal local;
        if (top.intValue() == 1703) {
            local = obterLocalDestino(jdbc, nuNota);
        } else {
            local = codLocalOrig;
        }

        // Atualiza CODLOCAL do item bipado
        NativeSql upd2 = new NativeSql(jdbc);
        upd2.appendSql("UPDATE AD_FTICODBARRAITEM SET CODLOCAL = :LOCAL" + (top.intValue() == 1701 ? ", ATIVO='N'" : "") + " WHERE CODBARRA = :ETI AND ATIVO='S'");
        upd2.setNamedParameter("LOCAL", local);
        upd2.setNamedParameter("ETI", codBarraItem);
        upd2.executeUpdate();

        // Desmonta palete se item pertencer a um palete existente
        NativeSql sqlPalete = new NativeSql(jdbc);
        sqlPalete.appendSql("SELECT CODBARRAPALETE FROM AD_FTICODBARRAITEM WHERE CODBARRA = :ETI AND CODBARRAPALETE IS NOT NULL");
        sqlPalete.setNamedParameter("ETI", codBarraItem);
        try (ResultSet rs = sqlPalete.executeQuery()) {
            if (rs.next()) {
                String codPalete = rs.getString("CODBARRAPALETE");
                NativeSql desativa = new NativeSql(jdbc);
                desativa.appendSql("UPDATE AD_FTICODBARRAPALETE SET ATIVO='N' WHERE CODBARRAPALETE = :PAL");
                desativa.setNamedParameter("PAL", codPalete);
                desativa.executeUpdate();
            }
        }

        if (seqVol == null) return;

        Integer itensPorPalete = obterQtdPorPalete(jdbc, codProd, codVol);
        if (itensPorPalete == null || itensPorPalete <= 0) return;

        int totalJaGravados = contarItensIvc(jdbc, nuNota, codProd, codVol, seqVol) + 1;

        // Mantém CX e CN como estão
        String tipoPalete = codVol;

        String seqPalete3 = String.format("%03d", seqVol.intValue());
        String codBarraPalete = codProd.toPlainString() + tipoPalete + "-" + nuNota.toPlainString() + "-" + seqPalete3;

        // Atualiza código de barras do palete
        NativeSql upd = new NativeSql(jdbc);
        upd.appendSql("UPDATE AD_FTICODBARRAITEM SET CODBARRAPALETE = :PAL WHERE CODBARRA = :ETI AND CODBARRAPALETE IS NULL");
        upd.setNamedParameter("PAL", codBarraPalete);
        upd.setNamedParameter("ETI", codBarraItem);
        upd.executeUpdate();

        // Insere e imprime palete completo
        if (itensPorPalete == totalJaGravados) {
            NativeSql ins = new NativeSql(jdbc);
            ins.appendSql(
                    "INSERT INTO AD_FTICODBARRAPALETE (NROCONFIG, CODBARRAPALETE, CODPROD, CODVOL, QTD, DHINC, CODUSU, CODLOCAL, NUNOTA, ATIVO) " +
                            "VALUES (1, :CODBARRA, :CODPROD, :CODVOL, :QTD, SYSDATE, :CODUSU, :LOCAL, :NUNOTA, 'S')"
            );
            ins.setNamedParameter("CODBARRA", codBarraPalete);
            ins.setNamedParameter("CODPROD", codProd);
            ins.setNamedParameter("CODVOL", tipoPalete);
            ins.setNamedParameter("QTD", itensPorPalete); // quantidade do palete
            ins.setNamedParameter("CODUSU", codUsu);
            ins.setNamedParameter("LOCAL", local);
            ins.setNamedParameter("NUNOTA", nuNota);
            ins.executeUpdate();

            String localPrinterName = obterImpressora(jdbc, local);

            PlatformService reportService = PlatformServiceFactory.getInstance().lookupService("@core:report.service");
            reportService.set("printer.name", localPrinterName);
            reportService.set("nurfe", 286);
            reportService.set("codemp", BigDecimal.ONE);

            Map<String, Object> parameters = new HashMap<>();
            parameters.put("P_CODBARRA", codBarraPalete);
            reportService.set("report.params", parameters);

            reportService.execute();
        }
    }


    private String obterImpressora(JdbcWrapper jdbc, BigDecimal codLocal) throws Exception {
        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql("SELECT IMPRESSORA FROM AD_FTICONFERENCIAIMPRES WHERE CODLOCAL = :CODLOCAL");
        sql.setNamedParameter("CODLOCAL", codLocal);
        try (ResultSet rs = sql.executeQuery()) {
            if (rs.next()) return rs.getString("IMPRESSORA");
        }
        return "?";
    }

    private BigDecimal obterLocalDestino(JdbcWrapper jdbc, BigDecimal nuNota) throws Exception {
        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql("SELECT NVL(CODLOCALDEST,CODLOCAL) DESTINO FROM AD_FTICONFERENCIA WHERE NUNOTACONF = :NUNOTACONF");
        sql.setNamedParameter("NUNOTACONF", nuNota);
        try (ResultSet rs = sql.executeQuery()) {
            if (rs.next()) return rs.getBigDecimal("DESTINO");
        }
        return null;
    }
}
