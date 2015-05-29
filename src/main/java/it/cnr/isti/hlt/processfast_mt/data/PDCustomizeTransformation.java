package it.cnr.isti.hlt.processfast_mt.data;

import it.cnr.isti.hlt.processfast.utils.Procedure1;


public class PDCustomizeTransformation implements PDBaseTransformation {
    @Override
    public boolean isRealTransformation() {
        return false;
    }

    public Procedure1<GParsPartitionableDataset> getCustomizationCode() {
        return customizationCode;
    }

    public void setCustomizationCode(Procedure1<GParsPartitionableDataset> customizationCode) {
        this.customizationCode = customizationCode;
    }

    /**
     * The closure customization code. The closure takes the owning partitionable dataset as argument.
     */
    private Procedure1<GParsPartitionableDataset> customizationCode;
}
