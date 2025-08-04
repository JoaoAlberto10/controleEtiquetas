import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.QueryExecutor;
import br.com.sankhya.extensions.actionbutton.Registro;
import br.com.sankhya.modelcore.PlatformService;
import br.com.sankhya.modelcore.PlatformServiceFactory;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class botaoImprimirEtiqueta implements AcaoRotinaJava{
    @Override
    public void doAction(ContextoAcao ctx) throws Exception {
        Registro[] linhasSelecionadas = ctx.getLinhas();
        PlatformService reportService = PlatformServiceFactory.getInstance().lookupService("@core:report.service");
        String p_nuNota = (String) ctx.getParam("P_NUNOTA");

        reportService.set("printer.name", "ZD230CPO01");
        reportService.set("codemp", BigDecimal.valueOf(1) );

                Map<String, Object> parameters = new HashMap<String, Object>();

                //parameters.put("P_NUNOTA", linha.getCampo("CODPROD"));
                parameters.put("P_QUANT_ETIQ", ctx.getParam("P_QUANT_ETIQ"));
                parameters.put("P_CODEMP2", ctx.getParam("P_CODEMP2"));

                reportService.set("report.params", parameters);
                System.out.println("Executando o relatório - ASM_SNK");
                reportService.execute();

    }
}