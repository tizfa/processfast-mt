package it.cnr.isti.hlt.processfast_mt.data;

import it.cnr.isti.hlt.processfast.data.ImmutableDataSourceIteratorProvider;

import java.io.Serializable;

/**
 * A PD results storage iterator.
 *
 * @param < T >  The type of data contained in the storage.
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
public interface PDResultsStorageIteratorProvider<T extends Serializable> extends ImmutableDataSourceIteratorProvider<T> {
    /**
     * Get the linked storage.
     *
     * @return The linked storage.
     */
    Object getStorage();
}
