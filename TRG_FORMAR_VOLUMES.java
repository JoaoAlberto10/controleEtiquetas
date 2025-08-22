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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TRG_FORMAR_VOLUMES implements EventoProgramavelJava {

    @Override public void beforeUpdate(PersistenceEvent event) throws Exception {}
    @Override public void afterUpdate(PersistenceEvent event) throws Exception {}
    @Override public void beforeDelete(PersistenceEvent event) {}
    @Override public void afterInsert(PersistenceEvent event) {}
    @Override public void afterDelete(PersistenceEvent event) {}
    @Override public void beforeCommit(TransactionContext tranCtx) {}

    // Evento esperado: BEFORE INSERT em TGFIVC
    @Override
    public void beforeInsert(PersistenceEvent event) throws Exception {
        DynamicVO vo = (DynamicVO) event.getVo();
        JdbcWrapper jdbc = event.getJdbcWrapper();

        final BigDecimal nuConf  = vo.asBigDecimal("NUCONF");
        final BigDecimal codProd = vo.asBigDecimal("CODPROD");
        final BigDecimal seqVol  = vo.asBigDecimal("SEQVOL");
        final String     codVol  = vo.asString("CODVOL");
        final String     codBarraItem = vo.asString("CODBARRA");

        if (nuConf == null || codProd == null || codVol == null) {
            throw new MGEModelException("NUCONF, CODPROD e CODVOL são obrigatórios para formação de palete.");
        }

        // 1) capacidade do palete (TGFVOA.AD_QTDPALETE)
        final Integer itensPorPalete = obterQtdPorPalete(jdbc, codProd, codVol);
        if (itensPorPalete == null || itensPorPalete <= 0) {
            return;
        }

        // 2) total atual de itens já gravados (sem contar o que está inserindo agora)
        final int totalJaGravados = contarItensIvc(jdbc, nuConf, codProd, codVol);
        final int totalComAtual   = totalJaGravados + 1;

        // 3) Se fechou palete
        if (totalComAtual % itensPorPalete != 0) {
            return; // ainda não fechou
        }

        // 3.1) tipo do palete a partir do volume do item
        final String tipoPalete;
        if ("CX".equalsIgnoreCase(codVol))       tipoPalete = "PX";
        else if ("CN".equalsIgnoreCase(codVol))  tipoPalete = "PN";
        else                                     tipoPalete = codVol; // fallback: mesmo volume

        // 3.2) número sequencial do palete dentro do grupo
        String seqPalete3 = String.format("%03d", seqVol.intValue());

        // 3.3) construir CODBARRA do palete: <CODPROD><tipoPalete>-<NUCONF>-<seqPalete3>
        final String codBarraPalete = codProd.toPlainString() + tipoPalete + "-" +
                nuConf.toPlainString() + "-" + seqPalete3;

        // 3.4) contexto opcional para preencher TGFBAR do palete (não obriga TGFCAB)
        final PaleteContext ctx = obterContexto(jdbc, codBarraItem, nuConf);

        // 3.5) pegar as N últimas etiquetas de item desse grupo (NUCONF+CODPROD+CODVOL) ainda sem AD_CODBARRAPALETE
        final List<String> ultimasEtiquetas = ultimasNEtiquetasItensPorConfProdVol(jdbc, nuConf, codProd, codVol, itensPorPalete);

        // 3.6) marcar itens com AD_CODBARRAPALETE
        for (String etq : ultimasEtiquetas) {
            NativeSql upd = new NativeSql(jdbc);
            upd.appendSql("UPDATE TGFBAR SET AD_CODBARRAPALETE = :PAL WHERE CODBARRA = :ETI AND AD_CODBARRAPALETE IS NULL");
            upd.setNamedParameter("PAL", codBarraPalete);
            upd.setNamedParameter("ETI", etq);
            upd.executeUpdate();
        }

        // 3.7) inserir etiqueta do palete em TGFBAR (se ainda não existir)
        if (!existeCodBarra(jdbc, codBarraPalete)) {
            /*NativeSql ins = new NativeSql(jdbc);
            ins.appendSql(
                    "INSERT INTO TGFBAR (CODBARRA, CODPROD, CODVOL, DHALTER, CODUSU, AD_LOCAL, AD_NUNOTA) " +
                            "VALUES (:CODBARRA, :CODPROD, :CODVOL, SYSDATE, :CODUSU, :AD_LOCAL, :AD_NUNOTA)"
            );
            ins.setNamedParameter("CODBARRA", codBarraPalete);
            ins.setNamedParameter("CODPROD",  codProd);
            ins.setNamedParameter("CODVOL",   tipoPalete);      // PX/PN (ou fallback)
            ins.setNamedParameter("CODUSU",   ctx.codUsu != null ? ctx.codUsu : BigDecimal.ZERO);
            ins.setNamedParameter("AD_LOCAL", ctx.codLocal);
            ins.setNamedParameter("AD_NUNOTA",ctx.nunota);
            ins.executeUpdate();*/

            String localPrinterName = "168.90.183.146:9091/ZD230CPO01";
            PlatformService reportService = PlatformServiceFactory.getInstance().lookupService("@core:report.service");
            reportService.set("printer.name", "168.90.183.146:9091/ZD230CPO01"); // localPrinterName
            reportService.set("nurfe", 287);
            reportService.set("codemp", BigDecimal.ONE);


            Map<String, Object> parameters = new HashMap<>();
            parameters.put("P_CODBARRA", "103033PX-435633-002");
            reportService.set("report.params", parameters);
            System.out.println("Executando o relatório - ASM_SNK");
            reportService.execute();
        }

    }

    // ===== Helpers =====

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

    private int contarItensIvc(JdbcWrapper jdbc, BigDecimal nuConf, BigDecimal codProd, String codVol) throws Exception {
        NativeSql ns = new NativeSql(jdbc);
        ns.appendSql("SELECT COUNT(*) FROM TGFIVC WHERE NUCONF = :C1 AND CODPROD = :C2 AND CODVOL = :C3");
        ns.setNamedParameter("C1", nuConf);
        ns.setNamedParameter("C2", codProd);
        ns.setNamedParameter("C3", codVol);
        ResultSet rs = ns.executeQuery();
        int c = 0;
        if (rs.next()) c = rs.getInt(1);
        rs.close();
        return c;
    }

    // Busca as N últimas etiquetas de item do grupo (NUCONF+CODPROD+CODVOL) que ainda não têm AD_CODBARRAPALETE
    // Ordena por TGFBAR.DHALTER desc para pegar as mais recentes fisicamente criadas.
    private List<String> ultimasNEtiquetasItensPorConfProdVol(JdbcWrapper jdbc, BigDecimal nuConf, BigDecimal codProd, String codVol, int n) throws Exception {
        List<String> lista = new ArrayList<>();
        NativeSql ns = new NativeSql(jdbc);
        ns.appendSql(
                "SELECT CODBARRA FROM (" +
                        "  SELECT b.CODBARRA, ROW_NUMBER() OVER (ORDER BY b.DHALTER DESC) RN " +
                        "  FROM TGFIVC i " +
                        "  JOIN TGFBAR b ON b.CODBARRA = i.CODBARRA " +
                        "  WHERE i.NUCONF = :NU AND i.CODPROD = :CP AND i.CODVOL = :CV AND (b.AD_CODBARRAPALETE IS NULL)" +
                        ") WHERE RN <= :LIM"
        );
        ns.setNamedParameter("NU",  nuConf);
        ns.setNamedParameter("CP",  codProd);
        ns.setNamedParameter("CV",  codVol);
        ns.setNamedParameter("LIM", n);
        ResultSet rs = ns.executeQuery();
        while (rs.next()) lista.add(rs.getString("CODBARRA"));
        rs.close();
        return lista;
    }

    private boolean existeCodBarra(JdbcWrapper jdbc, String codBarra) throws Exception {
        NativeSql ns = new NativeSql(jdbc);
        ns.appendSql("SELECT 1 FROM TGFBAR WHERE CODBARRA = :B");
        ns.setNamedParameter("B", codBarra);
        ResultSet rs = ns.executeQuery();
        boolean ok = rs.next();
        rs.close();
        return ok;
    }

    private static class PaleteContext {
        BigDecimal nunota;
        BigDecimal codUsu;
        BigDecimal codLocal;
    }

    // Tenta obter dados úteis para preencher a TGFBAR do palete:
    // 1) Pela etiqueta do item (TGFBAR)
    // 2) Pelo NUCONF -> TGFCON2.NUNOTAORIG (apenas leitura; não cria/atualiza nada)
    private PaleteContext obterContexto(JdbcWrapper jdbc, String codBarraItem, BigDecimal nuConf) throws Exception {
        PaleteContext ctx = new PaleteContext();

        // (1) Puxar da própria etiqueta do item, se existir
        if (codBarraItem != null) {
            NativeSql ns = new NativeSql(jdbc);
            ns.appendSql("SELECT MAX(AD_NUNOTA) NUNOTA, MAX(CODUSU) CODUSU, MAX(AD_LOCAL) AD_LOCAL FROM TGFBAR WHERE CODBARRA = :B");
            ns.setNamedParameter("B", codBarraItem);
            ResultSet rs = ns.executeQuery();
            if (rs.next()) {
                ctx.nunota   = rs.getBigDecimal("NUNOTA");
                ctx.codUsu   = rs.getBigDecimal("CODUSU");
                ctx.codLocal = rs.getBigDecimal("AD_LOCAL");
            }
            rs.close();
        }

        // (2) Se não achou NUNOTA, tenta herdar de TGFCON2.NUNOTAORIG (somente leitura)
        if (ctx.nunota == null) {
            NativeSql ns2 = new NativeSql(jdbc);
            ns2.appendSql("SELECT NUNOTAORIG FROM TGFCON2 WHERE NUCONF = :C AND ROWNUM = 1");
            ns2.setNamedParameter("C", nuConf);
            ResultSet rs2 = ns2.executeQuery();
            if (rs2.next()) ctx.nunota = rs2.getBigDecimal(1);
            rs2.close();
        }

        return ctx;
    }
}
