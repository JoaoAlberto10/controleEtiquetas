//package br.com.sankhya.action.eventosprogramaveis;

import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.event.TransactionContext;
import br.com.sankhya.modelcore.PlatformService;
import br.com.sankhya.modelcore.PlatformServiceFactory;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class imprimirEtiquetas implements EventoProgramavelJava {

    @Override
    public void afterUpdate(PersistenceEvent event) throws Exception {
        DynamicVO notaVO = (DynamicVO) event.getVo();
        DynamicVO notaVOold = (DynamicVO) event.getVo();
        String situacaoAtual = notaVO.asString("STATUSNOTA");
        String situacaoAnterior = notaVOold.asString("STATUSNOTA");
        int TOP = notaVO.asInt("CODTIPOPER");

        // Verifica se a nota foi alterada de rascunho (ou outro status) para confirmada
        if ((!"L".equals(situacaoAnterior) && "L".equals(situacaoAtual)) && TOP == 406) {
            int nuNota = notaVO.asInt("NUNOTA");
            BigDecimal codEmp = notaVO.asBigDecimal("CODEMP");
            String localPrinterName = "ZD230CPO01";

            PlatformService reportService = PlatformServiceFactory.getInstance().lookupService("@core:report.service");
            // reportService.set("printer.name", localPrinterName);
            reportService.set("codemp", codEmp);

            Map<String, Object> parameters = new HashMap<String, Object>();
            parameters.put("P_NUNOTA", nuNota);
            reportService.set("report.params", parameters);
            System.out.println("Executando o relatório - ASM_SNK");
            reportService.execute();

        }
    }

    @Override public void beforeInsert(PersistenceEvent event) {}
    @Override public void beforeUpdate(PersistenceEvent event) {}
    @Override public void beforeDelete(PersistenceEvent event) {}
    @Override public void afterInsert(PersistenceEvent event) {}
    @Override public void afterDelete(PersistenceEvent event) {}
    @Override public void beforeCommit(TransactionContext tranCtx) {}
}
