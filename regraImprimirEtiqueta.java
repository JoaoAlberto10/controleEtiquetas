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

        NativeSql cabecalho = new NativeSql(contexto.getJdbcWrapper());
        cabecalho.appendSql("SELECT CODUSUINC, CODEMP FROM TGFCAB WHERE NUNOTA = :NUNOTA");
        cabecalho.setNamedParameter("NUNOTA", nuNota);
        ResultSet cab = cabecalho.executeQuery();

        if (cab.next()) {
            BigDecimal codUsu = cab.getBigDecimal("CODUSUINC");
            BigDecimal codEmp = cab.getBigDecimal("CODEMP");
            BigDecimal novoNum = gerarNovoNumero(contexto.getJdbcWrapper(), "TGFCON2", codEmp);
            inserirConferencia(contexto.getJdbcWrapper(), novoNum, contexto.getNunota(), codUsu);
            System.out.println("Registro TGFCON2 inserido com sucesso.");
        } else {
            System.err.println("Cabeçalho da nota " + nuNota + " não encontrado.");
        }
        cab.close();


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

    private BigDecimal gerarNovoNumero(JdbcWrapper jdbc, String arquivo, BigDecimal codEmp) throws Exception {
        BigDecimal novoNumero = null;

        // 1. Seleciona com trava
        NativeSql select = new NativeSql(jdbc);
        select.appendSql("SELECT T.ULTCOD FROM TGFNUM T WHERE T.ARQUIVO = :ARQ AND T.CODEMP = :EMP FOR UPDATE");
        select.setNamedParameter("ARQ", arquivo);
        select.setNamedParameter("EMP", codEmp);
        ResultSet rs = select.executeQuery();

        if (rs.next()) {
            BigDecimal ultCod = rs.getBigDecimal("ULTCOD");
            novoNumero = ultCod.add(BigDecimal.ONE);

            // 2. Atualiza TGFNUM com novo código
            NativeSql update = new NativeSql(jdbc);
            update.appendSql("UPDATE TGFNUM SET ULTCOD = :NOVO WHERE ARQUIVO = :ARQ AND CODEMP = :EMP");
            update.setNamedParameter("NOVO", novoNumero);
            update.setNamedParameter("ARQ", arquivo);
            update.setNamedParameter("EMP", codEmp);
            update.executeUpdate();
        }
        rs.close();

        return novoNumero;
    }

    private void inserirConferencia(JdbcWrapper jdbc, BigDecimal nuConf, BigDecimal nuNota, BigDecimal codUsuConf) throws Exception {
        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql("INSERT INTO TGFCON2 (CODUSUCONF, DHFINCONF, DHINICONF, NUCONF, NUCONFORIG, NUNOTADEV, NUNOTAORIG, NUPEDCOMP, QTDVOL, STATUS) " +
                "VALUES (:CODUSUCONF, NULL, SYSDATE, :NUCONF, NULL, NULL, :NUNOTA, NULL, 0, 'A')");
        sql.setNamedParameter("CODUSUCONF", codUsuConf);
        sql.setNamedParameter("NUCONF", nuConf);
        sql.setNamedParameter("NUNOTA", nuNota);
        sql.executeUpdate();

        NativeSql sql2 = new NativeSql(jdbc);
        sql2.appendSql("UPDATE TGFCAB SET NUCONFATUAL = :NUCONF WHERE NUNOTA = :NUNOTA");
        sql2.setNamedParameter("NUCONF", nuConf);
        sql2.setNamedParameter("NUNOTA", nuNota);
        sql2.executeUpdate();
    }

    private void gerarEtiquetas(BigDecimal nuNota, JdbcWrapper jdbc) throws Exception {
        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql(
                "SELECT CODPROD, CODVOL, CODBARRA, CODLOCALORIG, CODUSUINC, SUM(QUANTIDADE) AS QUANTIDADE FROM (SELECT ITE.CODPROD, ITE.CODVOL, " +
                        "CASE WHEN ITE.CODVOL = PRO.CODVOL THEN ITE.QTDNEG ELSE ITE.QTDNEG / VOA.QUANTIDADE END AS QUANTIDADE, " +
                        "VOA.CODBARRA, ITE.CODLOCALORIG, CAB.CODUSUINC " +
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
