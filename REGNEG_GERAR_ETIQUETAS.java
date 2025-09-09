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

public class REGNEG_GERAR_ETIQUETAS implements RegraNegocioJava {
    public void executa(ContextoRegra contexto) throws Exception {
        BigDecimal nuNota = contexto.getNunota();
        try {
            gerarEtiquetas(nuNota, contexto.getJdbcWrapper());
            System.out.println("Etiquetas geradas com sucesso.");
        } catch (Exception e) {
            System.err.println("Erro ao gerar etiquetas: " + e.getMessage());
            return;
        }

        NativeSql cabecalho = new NativeSql(contexto.getJdbcWrapper());
        cabecalho.appendSql("SELECT CODUSUINC, CODEMP, CODTIPOPER, AD_CODLOCALORIG FROM TGFCAB WHERE NUNOTA = :NUNOTA");
        cabecalho.setNamedParameter("NUNOTA", nuNota);
        ResultSet cab = cabecalho.executeQuery();

        BigDecimal codLocal = null;

        if (cab.next()) {
            codLocal = cab.getBigDecimal("AD_CODLOCALORIG");
            System.out.println("Registro AD_FTICONFERENCIA inserido com sucesso.");
        } else {
            System.err.println("Cabeçalho da nota " + nuNota + " não encontrado.");
        }
        cab.close();

        String localPrinterName = obterImpressora(contexto.getJdbcWrapper(), codLocal);
        PlatformService reportService = PlatformServiceFactory.getInstance().lookupService("@core:report.service");
        reportService.set("printer.name", localPrinterName); // localPrinterName
        reportService.set("nurfe", 285);
        reportService.set("codemp", BigDecimal.ONE);

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("P_NUNOTA", nuNota.toPlainString());
        reportService.set("report.params", parameters);
        System.out.println("Executando o relatório - ASM_SNK");
        reportService.execute();
    }

    private static class Parametros {
        String tipoConf;      // 'E' ou 'S'
        String formaVolumes;  // ex.: 'S'
        int topConf;          // 1700 ou 1701
    }



    private void gerarEtiquetas(BigDecimal nuNota, JdbcWrapper jdbc) throws Exception {
        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql(
                "SELECT CODPROD, CODVOL, CODBARRA, CODLOCALORIG, CODUSUINC, SUM(QUANTIDADE) AS QUANTIDADE FROM (SELECT ITE.CODPROD, ITE.CODVOL, " +
                        "CASE WHEN ITE.CODVOL = PRO.CODVOL THEN ITE.QTDNEG ELSE ITE.QTDNEG / VOA.QUANTIDADE END AS QUANTIDADE, " +
                        "VOA.AD_CODBARRAINTERNO AS CODBARRA, ITE.CODLOCALORIG, CAB.CODUSUINC " +
                        "FROM TGFCAB CAB " +
                        "JOIN TGFITE ITE ON ITE.NUNOTA = CAB.NUNOTA " +
                        "JOIN TGFPRO PRO ON ITE.CODPROD = PRO.CODPROD " +
                        "JOIN TGFVOA VOA ON VOA.CODPROD = PRO.CODPROD AND VOA.CODVOL = ITE.CODVOL " +
                        "WHERE ITE.NUNOTA = :NUNOTA) GROUP BY CODPROD, CODVOL, CODBARRA, CODLOCALORIG, CODUSUINC"
        );
        sql.setNamedParameter("NUNOTA", nuNota);
        ResultSet rs = sql.executeQuery();

        while (rs.next()) {
            BigDecimal codProd = rs.getBigDecimal("CODPROD");
            String codVol = rs.getString("CODVOL");
            String codBarra = rs.getString("CODBARRA");
            int quantidade = rs.getBigDecimal("QUANTIDADE").intValue();
            BigDecimal codLocal = rs.getBigDecimal("CODLOCALORIG");
            BigDecimal codUsu = rs.getBigDecimal("CODUSUINC");

            for (int seq = 1; seq <= quantidade; seq++) {
                String codBarraGerado = codBarra + "-" + nuNota + "-" + String.format("%03d", seq);

                NativeSql insertSql = new NativeSql(jdbc);
                insertSql.appendSql("INSERT INTO AD_FTICODBARRAITEM (NROCONFIG, CODBARRA, CODPROD, CODVOL, DHINC, CODUSU, NUNOTA, ATIVO) " +
                        "VALUES (1, :CODBARRA, :CODPROD, :CODVOL, SYSDATE, :CODUSU, :NUNOTA, 'S')");
                insertSql.setNamedParameter("CODBARRA", codBarraGerado);
                insertSql.setNamedParameter("CODPROD", codProd);
                insertSql.setNamedParameter("CODVOL", codVol);
                insertSql.setNamedParameter("CODUSU", codUsu);
                insertSql.setNamedParameter("NUNOTA", nuNota);

                insertSql.executeUpdate();
                
            }
        }
        rs.close();
    }

    private String obterImpressora(JdbcWrapper jdbc, BigDecimal codLocal) throws Exception {
        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql("SELECT IMPRESSORA FROM AD_FTICONFERENCIAIMPRES WHERE CODLOCAL = :CODLOCAL");
        sql.setNamedParameter("CODLOCAL", codLocal);
        try (ResultSet rs = sql.executeQuery()) {
            if (rs.next()) {
                return rs.getString("IMPRESSORA");
            }
        }
        return "?";
    }
}
