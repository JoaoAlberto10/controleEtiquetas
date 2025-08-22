import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.Registro;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.MGEModelException;
import br.com.sankhya.modelcore.PlatformService;
import br.com.sankhya.modelcore.PlatformServiceFactory;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

public class BOTAO_REIMPRIMIR_ETIQUETAS implements AcaoRotinaJava {

    @Override
    public void doAction(ContextoAcao contexto) throws Exception {
        Registro[] regs = contexto.getLinhas();
        if (regs.length==0) {
            throw new MGEModelException("Nenhum produto foi selecionado!");
        }

        BigDecimal nuNota   = (BigDecimal) regs[0].getCampo("NUNOTA");
        BigDecimal codLocal = (BigDecimal) regs[0].getCampo("CODLOCALORIG");

        BigDecimal etqInicial = new BigDecimal((int) contexto.getParam("P_ETQINI"));
        BigDecimal etqFinal   = new BigDecimal((int) contexto.getParam("P_ETQFIN"));

        EntityFacade dwfEntityFacade = null;
        JdbcWrapper jdbc = null;
        dwfEntityFacade = EntityFacadeFactory.getDWFFacade();
        jdbc = dwfEntityFacade.getJdbcWrapper();
        jdbc.openSession();

        String localPrinterName = obterImpressora(jdbc, codLocal);

        PlatformService reportService = PlatformServiceFactory.getInstance().lookupService("@core:report.service");
        reportService.set("printer.name", localPrinterName); // localPrinterName
        reportService.set("nurfe", 285);
        reportService.set("codemp", BigDecimal.ONE);

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("P_NUNOTA", nuNota.toPlainString());
        parameters.put("P_NROETQ1", etqInicial.toPlainString());
        parameters.put("P_NROETQ2", etqFinal.toPlainString());
        reportService.set("report.params", parameters);
        System.out.println("Executando o relatório - ASM_SNK");
        reportService.execute();

        jdbc.closeSession();
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
