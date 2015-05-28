package it.cnr.isti.hlt.processfast_java8.data;

/**
 * Record handle for a base partitionable dataset transformation.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public interface PDBaseTransformation {
    /**
     * Indicate if this transformation or real or is just some modification of parameters used to
     * customize how the partitionable dataset is computed.
     *
     * @return True if this is a real PD transformation, false otherwise.
     */
    boolean isRealTransformation();
}
