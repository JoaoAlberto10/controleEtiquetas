//package br.com.sankhya.action.eventosprogramaveis;

import br.com.sankhya.extensions.regrasnegocio.ContextoRegra;
import br.com.sankhya.extensions.regrasnegocio.RegraNegocioJava;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.PlatformService;
import br.com.sankhya.modelcore.PlatformServiceFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

public class regraImprimirEtiqueta implements RegraNegocioJava {
    public void executa(ContextoRegra contexto) throws Exception {
        BigDecimal nuNota = contexto.getNunota();
        String localPrinterName = "ZD230CPO01";

        try {
            gerarEtiquetas(nuNota, contexto.getJdbcWrapper());
            System.out.println("Etiquetas geradas com sucesso.");
        } catch (Exception e) {
            System.err.println("Erro ao gerar etiquetas: " + e.getMessage());
            return;
        }

        PlatformService reportService = PlatformServiceFactory.getInstance().lookupService("@core:report.service");
        // reportService.set("printer.name", localPrinterName);
        reportService.set("nurfe", 283);
        reportService.set("codemp", BigDecimal.ONE);

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("P_NUNOTA", nuNota.toPlainString());
        reportService.set("report.params", parameters);
        System.out.println("Executando o relatório - ASM_SNK");
        reportService.execute();
    }

    private void gerarEtiquetas(BigDecimal nuNota, JdbcWrapper jdbc) throws Exception {
        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql(
                "SELECT ITE.CODPROD, ITE.CODVOL, " +
                        "CASE WHEN ITE.CODVOL = PRO.CODVOL THEN ITE.QTDNEG ELSE ITE.QTDNEG / VOA.QUANTIDADE END AS QUANTIDADE, " +
                        "VOA.CODBARRA, ITE.CODLOCALORIG, CAB.CODUSUINC " +
                        "FROM TGFCAB CAB " +
                        "JOIN TGFITE ITE ON ITE.NUNOTA = CAB.NUNOTA " +
                        "JOIN TGFPRO PRO ON ITE.CODPROD = PRO.CODPROD " +
                        "JOIN TGFVOA VOA ON VOA.CODPROD = PRO.CODPROD AND VOA.CODVOL = ITE.CODVOL " +
                        "WHERE ITE.NUNOTA = :NUNOTA"
        );
        sql.setNamedParameter("NUNOTA", nuNota);
        ResultSet rs = sql.executeQuery();

        while (rs.next()) {
            BigDecimal codProd = rs.getBigDecimal("CODPROD");
            String codVol = rs.getString("CODVOL");
            int quantidade = rs.getBigDecimal("QUANTIDADE").intValue();
            BigDecimal codLocal = rs.getBigDecimal("CODLOCALORIG");
            BigDecimal codUsu = rs.getBigDecimal("CODUSUINC");

            for (int seq = 1; seq <= quantidade; seq++) {
                String codBarraGerado = codProd + "-" + nuNota + "-" + String.format("%03d", seq);

                NativeSql insertSql = new NativeSql(jdbc);
                insertSql.appendSql("INSERT INTO TGFBAR (CODBARRA, CODPROD, CODVOL, DHALTER, CODUSU, AD_LOCAL, AD_NUNOTA) " +
                        "VALUES (:CODBARRA, :CODPROD, :CODVOL, SYSDATE, :CODUSU, :AD_LOCAL, :AD_NUNOTA)");
                insertSql.setNamedParameter("CODBARRA", codBarraGerado);
                insertSql.setNamedParameter("CODPROD", codProd);
                insertSql.setNamedParameter("CODVOL", codVol);
                insertSql.setNamedParameter("CODUSU", codUsu);
                insertSql.setNamedParameter("AD_LOCAL", codLocal);
                insertSql.setNamedParameter("AD_NUNOTA", nuNota);

                insertSql.executeUpdate();
            }
        }
        rs.close();
    }
}
