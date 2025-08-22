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
            BigDecimal codUsu = cab.getBigDecimal("CODUSUINC");
            BigDecimal codEmp = cab.getBigDecimal("CODEMP");
            BigDecimal codTop = cab.getBigDecimal("CODTIPOPER");
            codLocal = cab.getBigDecimal("AD_CODLOCALORIG");
            Parametros param = carregarParametros(contexto.getJdbcWrapper(), codTop.intValue());
            BigDecimal nuConf = gerarNovoNumero(contexto.getJdbcWrapper(), "AD_FTICONFERENCIA", codEmp);
            inserirConferencia(contexto.getJdbcWrapper(), nuConf, nuNota, codUsu, param.topConf, param.formaVolumes, codLocal);
            System.out.println("Registro AD_FTICONFERENCIA inserido com sucesso.");
        } else {
            System.err.println("Cabeçalho da nota " + nuNota + " não encontrado.");
        }
        cab.close();

        String localPrinterName = obterImpressora(contexto.getJdbcWrapper(), codLocal);

        PlatformService reportService = PlatformServiceFactory.getInstance().lookupService("@core:report.service");
        reportService.set("printer.name", localPrinterName); // localPrinterName
        reportService.set("nurfe", 283);
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

    private Parametros carregarParametros(JdbcWrapper jdbc, int codTop) throws Exception {
        Parametros p = new Parametros();

            NativeSql q = new NativeSql(jdbc);
            q.appendSql("SELECT TIPOCONFERENCIA, FORMAVOLUMES FROM AD_FTICONFERENCIAPARAM WHERE TOP = :TOP");
            q.setNamedParameter("TOP", codTop);
            try (ResultSet rs = q.executeQuery()) {
                if (rs.next()) {
                    p.tipoConf = rs.getString("TIPOCONFERENCIA");
                    p.formaVolumes = rs.getString("FORMAVOLUMES");
                } else {
                    // defaults se não houver linha
                    p.tipoConf = "E";
                    p.formaVolumes = "S";
                }
            }

        // regra solicitada: E => 1700, S => 1701
        p.topConf = "E".equalsIgnoreCase(p.tipoConf) ? 1700 : 1701;
        return p;
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

    private void inserirConferencia(JdbcWrapper jdbc, BigDecimal nuConf, BigDecimal nuNota, BigDecimal codUsuConf, int codTop, String formaVolume, BigDecimal codLocal) throws Exception {
        NativeSql sql = new NativeSql(jdbc);
        sql.appendSql("INSERT INTO AD_FTICONFERENCIA (NUCONF, NUNOTAORIG, DHINC, STATUS, CODUSU, TOPCONF, FORMAVOLUMES, CODLOCAL) " +
                "VALUES (:NUCONF, :NUNOTA,  SYSDATE, 'P', :CODUSU, :TOP, :FORMAVOLUMES, :CODLOCAL)");
        sql.setNamedParameter("CODUSU", codUsuConf);
        sql.setNamedParameter("NUNOTA", nuNota);
        sql.setNamedParameter("NUCONF", nuConf);
        sql.setNamedParameter("TOP", codTop);
        sql.setNamedParameter("FORMAVOLUMES", formaVolume);
        sql.setNamedParameter("CODLOCAL", codLocal);
        sql.executeUpdate();
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
                insertSql.appendSql("INSERT INTO AD_FTICODBARRAITEM (NROCONFIG, CODBARRA, CODPROD, CODVOL, DHINC, CODUSU, CODLOCAL, NUNOTA) " +
                        "VALUES (1, :CODBARRA, :CODPROD, :CODVOL, SYSDATE, :CODUSU, :CODLOCAL, :NUNOTA)");
                insertSql.setNamedParameter("CODBARRA", codBarraGerado);
                insertSql.setNamedParameter("CODPROD", codProd);
                insertSql.setNamedParameter("CODVOL", codVol);
                insertSql.setNamedParameter("CODUSU", codUsu);
                insertSql.setNamedParameter("CODLOCAL", codLocal);
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
